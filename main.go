package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	utls "github.com/refraction-networking/utls"
	"github.com/valyala/fasthttp"
)

// =================================================================
// الإعدادات الثابتة والبيئة
// =================================================================
var (
	telegramBotToken = os.Getenv("TELEGRAM_BOT_TOKEN")
	ownerTelegramID  int64
)

func init() {
	if raw := os.Getenv("OWNER_TELEGRAM_ID"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			ownerTelegramID = parsed
		}
	}
}

const (
	proxiesFilePath   = "data/proxies.txt"
	usernamesFilePath = "data/usernames.txt"
	seenFilePath      = "data/seen_held.txt"
	stateFilePath     = "data/state.txt"
	tempDirectory     = "data/temp"
	applicationLog    = "data/manus.log"
	debugResponsesLog = "data/last_responses.log"
	maxBodySize       = 1 << 20 // 1 MiB
)

var (
	configuredPoolSize   int32 = 10
	configuredPerUserCap int32 = 1
)

var baghdadTimezone = time.FixedZone("Baghdad", 3*60*60)

var (
	cycleMutex         sync.Mutex
	seenUsernames      = make(map[string]struct{})
	currentCycleHits   = make(map[string]struct{})
	usernameFirstSeen  = make(map[string]time.Time)
	usernameCheckTimes = make(map[string][2]time.Time)
	previousCycleEnd   time.Time
	isLoopRunning      int32
)

var (
	seenFileMutex  sync.Mutex
	stateFileMutex sync.Mutex
	logMutex       sync.Mutex
	debugLogMutex  sync.Mutex
	logFileHandle  *os.File
	debugLogHandle *os.File
)

var (
	appCtx, appCancel = context.WithCancel(context.Background())
)

var (
	cachedProxiesModTime   time.Time
	cachedUsernamesModTime time.Time
)

// =================================================================
// دالة قياس الموارد المدمجة (متوافقة مع الملف الواحد)
// =================================================================
func getPlatformSpecificMetrics() (float64, float64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	workingSetMB := float64(m.Sys) / (1024 * 1024)
	return workingSetMB, appStats.cpuUsagePercent
}

// =================================================================
// قناة النتائج ومرسل ديسكورد المستقل
// =================================================================
type ScanResult struct {
	Username  string
	FirstSeen time.Time
	FoundTime time.Time
}

var resultsChannel chan ScanResult
var resultsWorkerDone = make(chan struct{})

func StartResultsWriterAndNotifier() {
	defer close(resultsWorkerDone)

	file, err := os.OpenFile(seenFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		WriteLogEntry("[FILE WRITER ERROR] %v", err)
	}
	var fileWriter *os.File = file
	bufWriter := bufio.NewWriter(fileWriter)
	defer func() {
		if fileWriter != nil {
			bufWriter.Flush()
			fileWriter.Sync()
			fileWriter.Close()
		}
	}()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case res, ok := <-resultsChannel:
			if !ok {
				bufWriter.Flush()
				if fileWriter != nil {
					fileWriter.Sync()
				}
				return
			}
			if fileWriter != nil {
				seenFileMutex.Lock()
				bufWriter.WriteString(res.Username + "\n")
				seenFileMutex.Unlock()
			} else {
				SaveSeenUsernameToDatabase(res.Username)
			}

			NotifyDiscordAboutFoundUsernameAsync(res.Username, res.FirstSeen, res.FoundTime)
			WriteLogEntry("[AVAILABLE FOUND & SAVED] %s", res.Username)

		case <-ticker.C:
			seenFileMutex.Lock()
			bufWriter.Flush()
			if fileWriter != nil {
				fileWriter.Sync()
			}
			seenFileMutex.Unlock()

		case <-appCtx.Done():
			bufWriter.Flush()
			if fileWriter != nil {
				fileWriter.Sync()
			}
			return
		}
	}
}

// =================================================================
// مدير البروكسيات
// =================================================================
type ProxyNode struct {
	URL string
}

type ProxyRotationManager struct {
	mutex           sync.RWMutex
	nodes           []*ProxyNode
	rotationCounter int64
}

var proxyManager = &ProxyRotationManager{}

func (manager *ProxyRotationManager) LoadAndInitializeProxies(rawLines []string) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	var newNodes []*ProxyNode
	for _, rawProxy := range rawLines {
		cleanedProxy := strings.TrimSpace(rawProxy)
		if cleanedProxy != "" && !strings.HasPrefix(cleanedProxy, "#") {
			newNodes = append(newNodes, &ProxyNode{URL: cleanedProxy})
		}
	}
	manager.nodes = newNodes
	manager.rotationCounter = 0
}

func (manager *ProxyRotationManager) GetNextProxyNode() *ProxyNode {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	totalProxies := len(manager.nodes)
	if totalProxies == 0 {
		return nil
	}

	currentIndex := atomic.AddInt64(&manager.rotationCounter, 1)
	if currentIndex < 0 {
		currentIndex = -currentIndex
	}
	return manager.nodes[int(currentIndex)%totalProxies]
}

// =================================================================
// نظام الإحصائيات والأداء الشامل
// =================================================================
type ApplicationStatistics struct {
	mutex              sync.RWMutex
	loadedProxies      []string
	loadedUsernames    []string
	isRunning          bool
	totalUsernames     int
	totalProxiesCount  int
	completedTasks     int64
	availableFound     int64
	retryCount         int64
	totalRequestsSent  int64
	totalResponses     int64
	requestErrors      int64
	timeoutErrors      int64
	cancelErrors       int64
	networkErrors      int64
	tlsErrors          int64
	httpErrors         int64
	bodyLargeErrors    int64
	unknownErrors      int64
	spamCount          int64
	currentRPS         int64
	averageRPS         int64
	responseRPS        int64
	averageResponseRPS int64
	startTimeNano      int64
	lastSampleCount    int64
	lastResponseSample int64
	lastSampleNano     int64
	memoryUsageMB      float64
	rssMemoryMB        float64
	cpuUsagePercent    float64
	activeGoroutines   int
	pendingAction      string
}

var appStats = &ApplicationStatistics{}

var standardHTTPClient = &http.Client{
	Timeout: 6 * time.Second,
}

// =================================================================
// هياكل بيانات تيليجرام
// =================================================================
type TelegramUpdate struct {
	UpdateID      int              `json:"update_id"`
	IncomingMsg   *TelegramMessage `json:"message"`
	CallbackQuery *TelegramQuery   `json:"callback_query"`
}

type TelegramMessage struct {
	MessageID int           `json:"message_id"`
	Chat      TelegramChat  `json:"chat"`
	Text      string        `json:"text"`
	Document  *TelegramFile `json:"document"`
}

type TelegramChat struct {
	ID int64 `json:"id"`
}

type TelegramQuery struct {
	ID      string          `json:"id"`
	Sender  TelegramUser    `json:"from"`
	Message TelegramMessage `json:"message"`
	Data    string          `json:"data"`
}

type TelegramUser struct {
	ID int64 `json:"id"`
}

type TelegramFile struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MimeType string `json:"mime_type"`
	FileSize int    `json:"file_size"`
}

// =================================================================
// محرك الفحص المنظم
// =================================================================
type UsernameTask struct {
	TargetUsername string
	taskContext    context.Context
	cancelFunc     context.CancelFunc
	isFinished     int32
	activeWorkers  int32
	failureCount   int32
}

type UsernameScannerEngine struct {
	tasksList     []*UsernameTask
	taskChannel   chan *UsernameTask
	engineContext context.Context
	engineCancel  context.CancelFunc
	condLock      sync.Mutex
	condVar       *sync.Cond
	waitGroup     sync.WaitGroup
	totalTasks    int64
	finishedCount int64
	workerPool    int
	maxPerUser    int32
}

var activeScanner *UsernameScannerEngine
var scannerMutex sync.Mutex

func InitializeScannerEngine(ctx context.Context, usernames []string) *UsernameScannerEngine {
	engCtx, engCancel := context.WithCancel(ctx)
	var tasks []*UsernameTask
	for _, username := range usernames {
		tCtx, tCancel := context.WithCancel(engCtx)
		tasks = append(tasks, &UsernameTask{
			TargetUsername: username,
			taskContext:    tCtx,
			cancelFunc:     tCancel,
		})
	}

	poolSize := int(atomic.LoadInt32(&configuredPoolSize))
	userCap := atomic.LoadInt32(&configuredPerUserCap)

	engine := &UsernameScannerEngine{
		tasksList:     tasks,
		taskChannel:   make(chan *UsernameTask, poolSize*2),
		engineContext: engCtx,
		engineCancel:  engCancel,
		totalTasks:    int64(len(tasks)),
		workerPool:    poolSize,
		maxPerUser:    userCap,
	}
	engine.condVar = sync.NewCond(&engine.condLock)
	return engine
}

func (engine *UsernameScannerEngine) StartScanning() {
	for i := 0; i < engine.workerPool; i++ {
		engine.waitGroup.Add(1)
		go engine.workerRoutine()
	}
	engine.dispatchLoopWithoutPolling()
	engine.waitGroup.Wait()
}

func (engine *UsernameScannerEngine) dispatchLoopWithoutPolling() {
	defer close(engine.taskChannel)

	for {
		select {
		case <-engine.engineContext.Done():
			return
		case <-appCtx.Done():
			return
		default:
		}
		if atomic.LoadInt32(&isLoopRunning) == 0 {
			return
		}
		if atomic.LoadInt64(&engine.finishedCount) >= engine.totalTasks {
			return
		}

		task := engine.findNextEligibleTask()
		if task == nil {
			engine.condLock.Lock()
			if atomic.LoadInt64(&engine.finishedCount) < engine.totalTasks && atomic.LoadInt32(&isLoopRunning) == 1 {
				doneChan := engine.engineContext.Done()
				go func() {
					<-doneChan
					engine.condLock.Lock()
					engine.condVar.Broadcast()
					engine.condLock.Unlock()
				}()
				engine.condVar.Wait()
			}
			engine.condLock.Unlock()
			continue
		}

		select {
		case engine.taskChannel <- task:
		case <-engine.engineContext.Done():
			atomic.AddInt32(&task.activeWorkers, -1)
			return
		case <-appCtx.Done():
			atomic.AddInt32(&task.activeWorkers, -1)
			return
		}
	}
}

func (engine *UsernameScannerEngine) findNextEligibleTask() *UsernameTask {
	engine.condLock.Lock()
	defer engine.condLock.Unlock()

	numTasks := len(engine.tasksList)
	if numTasks == 0 {
		return nil
	}

	for i := 0; i < numTasks; i++ {
		task := engine.tasksList[i]
		if atomic.LoadInt32(&task.isFinished) == 1 {
			continue
		}
		for {
			currentWorkers := atomic.LoadInt32(&task.activeWorkers)
			if currentWorkers >= engine.maxPerUser {
				break
			}
			if atomic.CompareAndSwapInt32(&task.activeWorkers, currentWorkers, currentWorkers+1) {
				return task
			}
		}
	}
	return nil
}

func (engine *UsernameScannerEngine) workerRoutine() {
	defer engine.waitGroup.Done()
	defer func() {
		if recoveredError := recover(); recoveredError != nil {
			WriteLogEntry("[PANIC WORKER] %v", recoveredError)
		}
	}()
	for {
		select {
		case <-engine.engineContext.Done():
			return
		case <-appCtx.Done():
			return
		case task, ok := <-engine.taskChannel:
			if !ok {
				return
			}
			if atomic.LoadInt32(&task.isFinished) == 1 {
				atomic.AddInt32(&task.activeWorkers, -1)
				continue
			}
			engine.executeTaskProbe(task)
			atomic.AddInt32(&task.activeWorkers, -1)

			engine.condLock.Lock()
			engine.condVar.Signal()
			engine.condLock.Unlock()
		}
	}
}

func (engine *UsernameScannerEngine) executeTaskProbe(task *UsernameTask) {
	defer func() {
		if recoveredError := recover(); recoveredError != nil {
			WriteLogEntry("[PANIC PROBE %s] %v", task.TargetUsername, recoveredError)
		}
	}()
	if atomic.LoadInt32(&isLoopRunning) == 0 || atomic.LoadInt32(&task.isFinished) == 1 {
		return
	}

	proxyNode := proxyManager.GetNextProxyNode()
	var proxyURLStr string
	if proxyNode != nil {
		proxyURLStr = proxyNode.URL
	}

	checkTimestamp := time.Now().In(baghdadTimezone)
	cycleMutex.Lock()
	if _, exists := usernameFirstSeen[task.TargetUsername]; !exists {
		usernameFirstSeen[task.TargetUsername] = checkTimestamp
	}
	cycleMutex.Unlock()

	reqCtx, reqCancel := context.WithTimeout(engine.engineContext, 6*time.Second)
	defer reqCancel()

	verdict, isSpam, errType := executeGraphQLValidationSafe(reqCtx, task.TargetUsername, proxyURLStr)
	if isSpam {
		atomic.AddInt64(&appStats.spamCount, 1)
	}

	switch verdict {
	case "good":
		if atomic.CompareAndSwapInt32(&task.isFinished, 0, 1) {
			task.cancelFunc()
			atomic.AddInt64(&engine.finishedCount, 1)

			discoveryTime := time.Now().In(baghdadTimezone)
			cycleMutex.Lock()
			firstTime := usernameFirstSeen[task.TargetUsername]
			if firstTime.IsZero() {
				firstTime = checkTimestamp
			}
			usernameCheckTimes[task.TargetUsername] = [2]time.Time{firstTime, discoveryTime}
			currentCycleHits[task.TargetUsername] = struct{}{}
			cycleMutex.Unlock()

			resultsChannel <- ScanResult{
				Username:  task.TargetUsername,
				FirstSeen: firstTime,
				FoundTime: discoveryTime,
			}

			atomic.AddInt64(&appStats.availableFound, 1)
			atomic.AddInt64(&appStats.completedTasks, 1)
		}
	case "taken":
		if atomic.CompareAndSwapInt32(&task.isFinished, 0, 1) {
			task.cancelFunc()
			atomic.AddInt64(&engine.finishedCount, 1)

			discoveryTime := time.Now().In(baghdadTimezone)
			cycleMutex.Lock()
			firstTime := usernameFirstSeen[task.TargetUsername]
			if firstTime.IsZero() {
				firstTime = checkTimestamp
			}
			usernameCheckTimes[task.TargetUsername] = [2]time.Time{firstTime, discoveryTime}
			cycleMutex.Unlock()

			atomic.AddInt64(&appStats.completedTasks, 1)
		}
	default:
		atomic.AddInt64(&appStats.retryCount, 1)
		switch errType {
		case "timeout":
			atomic.AddInt64(&appStats.timeoutErrors, 1)
		case "cancel":
			atomic.AddInt64(&appStats.cancelErrors, 1)
		case "network":
			atomic.AddInt64(&appStats.networkErrors, 1)
		case "tls":
			atomic.AddInt64(&appStats.tlsErrors, 1)
		case "http":
			atomic.AddInt64(&appStats.httpErrors, 1)
		case "body_large":
			atomic.AddInt64(&appStats.bodyLargeErrors, 1)
		default:
			atomic.AddInt64(&appStats.unknownErrors, 1)
		}

		failures := atomic.AddInt32(&task.failureCount, 1)
		if failures > 3 {
			if atomic.CompareAndSwapInt32(&task.isFinished, 0, 1) {
				task.cancelFunc()
				atomic.AddInt64(&engine.finishedCount, 1)
				atomic.AddInt64(&appStats.completedTasks, 1)
			}
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (engine *UsernameScannerEngine) StopEngine() {
	engine.engineCancel()
	engine.condLock.Lock()
	engine.condVar.Broadcast()
	engine.condLock.Unlock()
}

// =================================================================
// تنفيذ فحص الـ GraphQL
// =================================================================
func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func fastHex(n int) string {
	hexChars := []rune("0123456789abcdef")
	b := make([]rune, n)
	for i := range b {
		b[i] = hexChars[rand.Intn(16)]
	}
	return string(b)
}

func WriteDebugResponseLog(proxyURL string, username string, statusCode int, responseSnippet string, err string) {
	debugLogMutex.Lock()
	defer debugLogMutex.Unlock()

	if debugLogHandle != nil {
		entry := fmt.Sprintf("[%s] USER: %s | PROXY: %s | STATUS: %d | ERR: %s | RESPONSE: %s\n",
			time.Now().Format("15:04:05"), username, proxyURL, statusCode, err, responseSnippet)
		debugLogHandle.WriteString(entry)
	}
}

func executeGraphQLValidationSafe(ctx context.Context, username string, proxyURLStr string) (string, bool, string) {
	var activeConn net.Conn

	client := &fasthttp.HostClient{
		Addr:                          "www.instagram.com:443",
		IsTLS:                         true,
		ReadBufferSize:                65536,
		DisableHeaderNamesNormalizing: true,
		Dial: func(addr string) (net.Conn, error) {
			var tcpConn net.Conn
			var err error

			dialsNet := &net.Dialer{}
			if proxyURLStr != "" {
				pURL, parseErr := url.Parse(proxyURLStr)
				if parseErr != nil {
					return nil, parseErr
				}
				tcpConn, err = dialsNet.DialContext(ctx, "tcp", pURL.Host)
				if err != nil {
					return nil, err
				}

				connectReq := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", addr, addr)
				_, err = tcpConn.Write([]byte(connectReq))
				if err != nil {
					tcpConn.Close()
					return nil, err
				}

				buf := make([]byte, 1024)
				n, err := tcpConn.Read(buf)
				if err != nil {
					tcpConn.Close()
					return nil, err
				}
				if !strings.Contains(string(buf[:n]), "200") {
					tcpConn.Close()
					return nil, fmt.Errorf("proxy connect failed: %s", string(buf[:n]))
				}
			} else {
				tcpConn, err = dialsNet.DialContext(ctx, "tcp", addr)
				if err != nil {
					return nil, err
				}
			}

			activeConn = tcpConn

			utlsConfig := &utls.Config{
				ServerName:         "www.instagram.com",
				InsecureSkipVerify: true,
			}

			utlsConn := utls.UClient(tcpConn, utlsConfig, utls.HelloChrome_120)
			if err := utlsConn.BuildHandshakeState(); err != nil {
				tcpConn.Close()
				return nil, err
			}

			for _, ext := range utlsConn.Extensions {
				if alpn, ok := ext.(*utls.ALPNExtension); ok {
					alpn.AlpnProtocols = []string{"http/1.1"}
					break
				}
			}

			if err := utlsConn.Handshake(); err != nil {
				tcpConn.Close()
				return nil, err
			}
			return utlsConn, nil
		},
	}

	lsd := "Ad" + randomString(9)
	docID := "25391252800555418"
	loxSol := fastHex(32)

	variablesJSON := fmt.Sprintf(`{"input":{"client_mutation_id":"1","field_name":"USERNAME","lox_pow_solution":"%s","username":{"sensitive_string_value":"%s"}},"scale":1}`, loxSol, username)

	postData := fmt.Sprintf("lsd=%s&variables=%s&doc_id=%s&fb_api_analytics_tags=%%5B%%22qpl_active_flow_ids%%3D516759801%%22%%5D",
		lsd, url.QueryEscape(variablesJSON), docID)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.Header.DisableNormalizing()
	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetRequestURI("https://www.instagram.com/api/graphql")

	req.Header.Set("Host", "www.instagram.com")
	req.Header.Set("Cookie", "ig_did=0")
	req.Header.Set("X-Fb-Lsd", lsd)
	req.Header.Set("X-Fb-Friendly-Name", "useCAARegistrationFieldValidationQuery")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Origin", "https://www.instagram.com")
	req.Header.Set("Referer", "https://www.instagram.com/accounts/emailsignup/")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("Accept-Encoding", "identity")

	req.SetBodyString(postData)

	atomic.AddInt64(&appStats.totalRequestsSent, 1)

	type resStruct struct {
		err error
	}
	doneChan := make(chan resStruct, 1)

	go func() {
		err := client.Do(req, resp)
		select {
		case doneChan <- resStruct{err: err}:
		default:
		}
	}()

	select {
	case <-ctx.Done():
		if activeConn != nil {
			activeConn.Close()
		}
		<-doneChan
		atomic.AddInt64(&appStats.requestErrors, 1)
		if ctx.Err() == context.DeadlineExceeded {
			return "error", false, "timeout"
		}
		return "error", false, "cancel"

	case res := <-doneChan:
		if res.err != nil {
			atomic.AddInt64(&appStats.requestErrors, 1)
			errStr := res.err.Error()
			errType := "network"
			if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline") {
				errType = "timeout"
			} else if strings.Contains(errStr, "tls") || strings.Contains(errStr, "handshake") {
				errType = "tls"
			} else if strings.Contains(errStr, "connection") {
				errType = "network"
			}
			WriteDebugResponseLog(proxyURLStr, username, 0, "", errStr)
			return "error", false, errType
		}
	}

	atomic.AddInt64(&appStats.totalResponses, 1)

	statusCode := resp.StatusCode()
	bodyBytes := resp.Body()

	if len(bodyBytes) > maxBodySize {
		if activeConn != nil {
			activeConn.Close()
		}
		WriteDebugResponseLog(proxyURLStr, username, statusCode, "OVERSIZED_BODY", "BODY_TOO_LARGE")
		return "error", false, "body_large"
	}

	var snippet string
	if len(bodyBytes) > 200 {
		snippet = string(bodyBytes[:200])
	} else {
		snippet = string(bodyBytes)
	}

	if statusCode == 429 || bytes.Contains(bodyBytes, []byte("feedback_required")) || bytes.Contains(bodyBytes, []byte("Please wait")) {
		WriteDebugResponseLog(proxyURLStr, username, statusCode, snippet, "SPAM_BLOCK_DETECTED")
		return "error", true, "http"
	}

	if bytes.Contains(bodyBytes, []byte(`"status":"SUCCESS"`)) {
		return "good", false, ""
	} else if bytes.Contains(bodyBytes, []byte(`"status":"VALIDATION_ERROR"`)) || bytes.Contains(bodyBytes, []byte("is not available")) {
		return "taken", false, ""
	}

	WriteDebugResponseLog(proxyURLStr, username, statusCode, snippet, "UNKNOWN_RESPONSE")
	return "taken", false, ""
}

// =================================================================
// مدير السجلات ووظائف الملفات
// =================================================================
func InitializeLogFiles() error {
	file, err := os.OpenFile(applicationLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	logFileHandle = file

	dFile, dErr := os.OpenFile(debugResponsesLog, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if dErr == nil {
		debugLogHandle = dFile
	}

	return nil
}

func WriteLogEntry(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	formattedLine := fmt.Sprintf("[%s] %s", time.Now().Format("2006-01-02 15:04:05"), message)
	fmt.Println(formattedLine)
	if logFileHandle != nil {
		logMutex.Lock()
		fmt.Fprintln(logFileHandle, formattedLine)
		logMutex.Unlock()
	}
}

func RemoveEmojisFromString(text string) string {
	var builder strings.Builder
	for _, r := range text {
		switch {
		case r >= 0x1F000 && r <= 0x1FAFF:
			continue
		case r >= 0x2600 && r <= 0x27BF:
			continue
		case r >= 0x2300 && r <= 0x23FF:
			continue
		case r == 0xFE0E || r == 0xFE0F:
			continue
		case r == 0x200D:
			continue
		default:
			builder.WriteRune(r)
		}
	}
	return strings.TrimSpace(builder.String())
}

func SanitizeTelegramKeyboard(keyboardMarkup interface{}) interface{} {
	switch typedValue := keyboardMarkup.(type) {
	case map[string]interface{}:
		sanitizedMap := make(map[string]interface{}, len(typedValue))
		for k, item := range typedValue {
			sanitizedMap[k] = SanitizeTelegramKeyboard(item)
		}
		return sanitizedMap
	case [][]map[string]string:
		newRows := make([][]map[string]string, len(typedValue))
		for i, row := range typedValue {
			newRows[i] = make([]map[string]string, len(row))
			for j, button := range row {
				newButton := make(map[string]string, len(button))
				for k, textVal := range button {
					if k == "text" {
						newButton[k] = RemoveEmojisFromString(textVal)
					} else {
						newButton[k] = textVal
					}
				}
				newRows[i][j] = newButton
			}
		}
		return newRows
	default:
		return keyboardMarkup
	}
}

func SanitizeUsernameString(username string) string {
	username = strings.TrimSpace(username)
	username = strings.TrimPrefix(username, "@")
	return strings.ToLower(username)
}

func RemoveDuplicateUsernamesInFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	seenMap := make(map[string]struct{})
	var uniqueLines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		cleanLine := SanitizeUsernameString(scanner.Text())
		if cleanLine == "" {
			continue
		}
		if _, exists := seenMap[cleanLine]; exists {
			continue
		}
		seenMap[cleanLine] = struct{}{}
		uniqueLines = append(uniqueLines, cleanLine)
	}

	tempFilePath := filePath + ".tmp"
	outputFile, err := os.Create(tempFilePath)
	if err != nil {
		return err
	}
	for _, line := range uniqueLines {
		if _, err := fmt.Fprintln(outputFile, line); err != nil {
			outputFile.Close()
			os.Remove(tempFilePath)
			return err
		}
	}
	if err := outputFile.Close(); err != nil {
		os.Remove(tempFilePath)
		return err
	}
	return os.Rename(tempFilePath, filePath)
}

func LoadProxiesFromFileIntoMemory() int {
	fileInfo, err := os.Stat(proxiesFilePath)
	if err == nil {
		if !fileInfo.ModTime().After(cachedProxiesModTime) && len(appStats.loadedProxies) > 0 {
			return len(appStats.loadedProxies)
		}
		cachedProxiesModTime = fileInfo.ModTime()
	}

	file, err := os.Open(proxiesFilePath)
	if err != nil {
		WriteLogEntry("[PROXY ERROR] Cannot open %s: %v", proxiesFilePath, err)
		return 0
	}
	defer file.Close()

	seenSet := make(map[string]struct{})
	var proxyLines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		proxyItem := strings.TrimSpace(scanner.Text())
		if proxyItem == "" || strings.HasPrefix(proxyItem, "#") {
			continue
		}
		if _, exists := seenSet[proxyItem]; exists {
			continue
		}
		seenSet[proxyItem] = struct{}{}
		proxyLines = append(proxyLines, proxyItem)
	}

	appStats.mutex.Lock()
	appStats.loadedProxies = proxyLines
	appStats.totalProxiesCount = len(proxyLines)
	appStats.mutex.Unlock()

	proxyManager.LoadAndInitializeProxies(proxyLines)
	return len(proxyLines)
}

func LoadUsernamesFromFileIntoMemory() int {
	fileInfo, err := os.Stat(usernamesFilePath)
	if err == nil {
		if !fileInfo.ModTime().After(cachedUsernamesModTime) && len(appStats.loadedUsernames) > 0 {
			return len(appStats.loadedUsernames)
		}
		cachedUsernamesModTime = fileInfo.ModTime()
	}

	file, err := os.Open(usernamesFilePath)
	if err != nil {
		WriteLogEntry("[USERS ERROR] Cannot open %s: %v", usernamesFilePath, err)
		return 0
	}
	defer file.Close()

	seenSet := make(map[string]struct{})
	var usernameLines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		u := SanitizeUsernameString(scanner.Text())
		if u == "" {
			continue
		}
		if _, exists := seenSet[u]; exists {
			continue
		}
		seenSet[u] = struct{}{}
		usernameLines = append(usernameLines, u)
	}

	appStats.mutex.Lock()
	appStats.loadedUsernames = usernameLines
	appStats.totalUsernames = len(usernameLines)
	appStats.mutex.Unlock()

	return len(usernameLines)
}

func LoadSeenUsernamesDatabase() {
	file, err := os.Open(seenFilePath)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		username := SanitizeUsernameString(scanner.Text())
		if username != "" {
			seenUsernames[username] = struct{}{}
		}
	}
	WriteLogEntry("[SEEN] Loaded %d verified usernames", len(seenUsernames))
}

func SaveSeenUsernameToDatabase(username string) {
	username = SanitizeUsernameString(username)
	if username == "" {
		return
	}
	seenFileMutex.Lock()
	defer seenFileMutex.Unlock()
	file, err := os.OpenFile(seenFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		fmt.Fprintln(file, username)
		file.Close()
	}
}

func SaveCheckerState() {
	stateFileMutex.Lock()
	defer stateFileMutex.Unlock()
	cycleMutex.Lock()
	endTime := previousCycleEnd
	cycleMutex.Unlock()

	file, err := os.OpenFile(stateFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		WriteLogEntry("[STATE ERROR] %v", err)
		return
	}
	defer file.Close()

	unixNanoVal := int64(0)
	if !endTime.IsZero() {
		unixNanoVal = endTime.UnixNano()
	}
	fmt.Fprintf(file, "%d\n", unixNanoVal)
}

func LoadCheckerState() {
	file, err := os.Open(stateFilePath)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		if nanoSecs, err := strconv.ParseInt(strings.TrimSpace(scanner.Text()), 10, 64); err == nil && nanoSecs > 0 {
			previousCycleEnd = time.Unix(0, nanoSecs)
			WriteLogEntry("[STATE] Loaded previous cycle end: %s", previousCycleEnd.In(baghdadTimezone).Format("2006-01-02 03:04:05 PM"))
		}
	}
}

// =================================================================
// إشعارات ديسكورد وبوت تيليجرام
// =================================================================
func GetDiscordWebhookURL() string {
	return os.Getenv("DISCORD_WEBHOOK_URL")
}

func SendDiscordLiveMessage(content string) string {
	webhookURL := GetDiscordWebhookURL() + "?wait=true"
	payloadMap := map[string]string{"content": content}
	marshaledData, _ := json.Marshal(payloadMap)

	httpRequest, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewReader(marshaledData))
	if err != nil {
		return ""
	}
	httpRequest.Header.Set("Content-Type", "application/json")

	httpResponse, err := standardHTTPClient.Do(httpRequest)
	if err != nil {
		return ""
	}
	defer httpResponse.Body.Close()

	responseBytes, _ := io.ReadAll(httpResponse.Body)
	var responseParsed struct {
		ID string `json:"id"`
	}
	json.Unmarshal(responseBytes, &responseParsed)
	return responseParsed.ID
}

func EditDiscordLiveMessage(messageID string, newContent string) error {
	if messageID == "" {
		return SendDiscordAlertMessage(newContent)
	}
	webhookURL := GetDiscordWebhookURL() + "/messages/" + messageID
	payloadMap := map[string]string{"content": newContent}
	marshaledData, _ := json.Marshal(payloadMap)

	httpRequest, err := http.NewRequest(http.MethodPatch, webhookURL, bytes.NewReader(marshaledData))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")

	httpResponse, err := standardHTTPClient.Do(httpRequest)
	if err != nil {
		return err
	}
	defer httpResponse.Body.Close()

	if httpResponse.StatusCode >= 200 && httpResponse.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("discord patch error: %s", httpResponse.Status)
}

func SendDiscordAlertMessage(content string) error {
	webhookURL := GetDiscordWebhookURL()
	if webhookURL == "" {
		return fmt.Errorf("discord webhook is not configured")
	}

	payloadMap := map[string]string{"content": content}
	marshaledData, err := json.Marshal(payloadMap)
	if err != nil {
		return err
	}

	httpRequest, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewReader(marshaledData))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")

	httpResponse, err := standardHTTPClient.Do(httpRequest)
	if err != nil {
		return err
	}
	defer httpResponse.Body.Close()

	if httpResponse.StatusCode == http.StatusNoContent || (httpResponse.StatusCode >= 200 && httpResponse.StatusCode < 300) {
		return nil
	}

	respBody, _ := io.ReadAll(httpResponse.Body)
	return fmt.Errorf("Discord HTTP %s: %s", httpResponse.Status, string(respBody))
}

func BuildLiveStatsDiscordText() string {
	appStats.mutex.RLock()
	totalRequests := atomic.LoadInt64(&appStats.totalRequestsSent)
	totalResponses := atomic.LoadInt64(&appStats.totalResponses)
	requestErrors := atomic.LoadInt64(&appStats.requestErrors)
	timeoutErrors := atomic.LoadInt64(&appStats.timeoutErrors)
	cancelErrors := atomic.LoadInt64(&appStats.cancelErrors)
	networkErrors := atomic.LoadInt64(&appStats.networkErrors)
	tlsErrors := atomic.LoadInt64(&appStats.tlsErrors)
	httpErrors := atomic.LoadInt64(&appStats.httpErrors)
	bodyLargeErrors := atomic.LoadInt64(&appStats.bodyLargeErrors)
	spamCount := atomic.LoadInt64(&appStats.spamCount)
	currentRPS := atomic.LoadInt64(&appStats.currentRPS)
	averageRPS := atomic.LoadInt64(&appStats.averageRPS)
	currentResponseRPS := atomic.LoadInt64(&appStats.responseRPS)
	averageResponseRPS := atomic.LoadInt64(&appStats.averageResponseRPS)
	allocatedRAM := appStats.memoryUsageMB
	rssRAM := appStats.rssMemoryMB
	cpuPct := appStats.cpuUsagePercent
	activeRoutines := appStats.activeGoroutines
	totalUsernamesCount := appStats.totalUsernames
	completedCount := atomic.LoadInt64(&appStats.completedTasks)
	appStats.mutex.RUnlock()

	progressPercent := 0.0
	if totalUsernamesCount > 0 {
		progressPercent = float64(completedCount) / float64(totalUsernamesCount) * 100
		if progressPercent > 100 {
			progressPercent = 100
		}
	}

	return fmt.Sprintf(
		"**Jekr Need Live Monitoring**\n"+
			"ATT: %d | Res: %d | Err: %d (TO:%d, Ccl:%d, Net:%d, TLS:%d, HTTP:%d, BigBody:%d)\n"+
			"Spam: %d\n"+
			"Sent RPS (now/avg): %d / %d\n"+
			"Resp RPS (now/avg): %d / %d\n"+
			"RAM (Heap/RSS): %.2f MB / %.2f MB\n"+
			"CPU: %.1f%% | Goroutines: %d\n"+
			"Progress: %.1f%%",
		totalRequests, totalResponses, requestErrors, timeoutErrors, cancelErrors, networkErrors, tlsErrors, httpErrors, bodyLargeErrors,
		spamCount, currentRPS, averageRPS, currentResponseRPS, averageResponseRPS,
		allocatedRAM, rssRAM, cpuPct, activeRoutines, progressPercent,
	)
}

func NotifyDiscordAboutFoundUsernameAsync(username string, firstSeen time.Time, foundTime time.Time) {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC DISCORD NOTIFY] %v", rec)
		}
	}()

	dateStr := firstSeen.Format("2006/01/02")
	t1Str := firstSeen.Format("15:04:05")
	t2Str := foundTime.Format("15:04:05")
	timeInfoFormatted := fmt.Sprintf("Time: `%s`\n`%s-%s`", dateStr, t1Str, t2Str)

	alertText := fmt.Sprintf(
		"🚨 **AVAILABLE USERNAME FOUND! (SUCCESS)**\n"+
			"Username: `%s`\n"+
			"%s\n"+
			"Timezone: Baghdad (GMT+3)",
		username, timeInfoFormatted,
	)
	if err := SendDiscordAlertMessage(alertText); err != nil {
		WriteLogEntry("[DISCORD ERROR] %v", err)
	}
}

func StartDiscordLiveStatusStream(doneSignal chan struct{}) {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC DISCORD STREAM] %v", rec)
		}
	}()

	messageID := SendDiscordLiveMessage(BuildLiveStatsDiscordText())
	tickerInterval := time.NewTicker(3 * time.Second)
	defer tickerInterval.Stop()

	for {
		select {
		case <-doneSignal:
			EditDiscordLiveMessage(messageID, BuildLiveStatsDiscordText()+"\n\n**[Cycle Completed - Restarting Loop...]**")
			return
		case <-tickerInterval.C:
			EditDiscordLiveMessage(messageID, BuildLiveStatsDiscordText())
		}
	}
}

func SendTelegramMessage(chatID int64, textContent string, inlineKeyboard interface{}) int {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC TELEGRAM SEND] %v", rec)
		}
	}()
	textContent = RemoveEmojisFromString(textContent)
	inlineKeyboard = SanitizeTelegramKeyboard(inlineKeyboard)

	apiEndpoint := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", telegramBotToken)
	payloadMap := map[string]interface{}{
		"chat_id":    chatID,
		"text":       textContent,
		"parse_mode": "HTML",
	}
	if inlineKeyboard != nil {
		payloadMap["reply_markup"] = inlineKeyboard
	}

	marshaledJSON, _ := json.Marshal(payloadMap)
	httpResponse, err := standardHTTPClient.Post(apiEndpoint, "application/json", bytes.NewBuffer(marshaledJSON))
	if err != nil {
		WriteLogEntry("[TELEGRAM ERROR] %v", err)
		return 0
	}
	defer httpResponse.Body.Close()

	responseBytes, _ := io.ReadAll(httpResponse.Body)
	var parsedResult struct {
		OK     bool `json:"ok"`
		Result struct {
			MessageID int `json:"message_id"`
		} `json:"result"`
	}
	json.Unmarshal(responseBytes, &parsedResult)
	return parsedResult.Result.MessageID
}

func DeleteTelegramMessage(chatID int64, messageID int) {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC TELEGRAM DELETE] %v", rec)
		}
	}()
	apiEndpoint := fmt.Sprintf("https://api.telegram.org/bot%s/deleteMessage", telegramBotToken)
	payloadMap := map[string]interface{}{"chat_id": chatID, "message_id": messageID}
	marshaledJSON, _ := json.Marshal(payloadMap)
	standardHTTPClient.Post(apiEndpoint, "application/json", bytes.NewBuffer(marshaledJSON))
}

func RenderMainMenu(chatID int64) {
	appStats.mutex.RLock()
	proxiesCount := len(appStats.loadedProxies)
	usernamesCount := len(appStats.loadedUsernames)
	isRunningActive := atomic.LoadInt32(&isLoopRunning) == 1
	appStats.mutex.RUnlock()

	statusString := "Stopped"
	if isRunningActive {
		statusString = "Running (Continuous Loop)"
	}

	poolSize := atomic.LoadInt32(&configuredPoolSize)
	userCap := atomic.LoadInt32(&configuredPerUserCap)

	dashboardText := fmt.Sprintf(
		"<b>Jekr Need Username Checker</b>\n\n"+
			"<b>Status:</b> %s\n"+
			"<b>Proxies:</b> %d\n"+
			"<b>Usernames:</b> %d\n"+
			"<b>Threads (Pool):</b> %d\n"+
			"<b>Per-User Cap:</b> %d\n\n"+
			"<i>Instant alerts go to Discord</i>",
		statusString, proxiesCount, usernamesCount, poolSize, userCap,
	)

	keyboardMarkup := map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": "Usernames", "callback_data": "usernames"}, {"text": "Proxies", "callback_data": "proxies"}},
			{{"text": "Threads (Pool)", "callback_data": "threads"}, {"text": "Per-User Cap", "callback_data": "user_cap"}},
			{
				{"text": "Start Loop", "callback_data": "start"},
				{"text": "Stop", "callback_data": "stop"},
			},
		},
	}
	SendTelegramMessage(chatID, dashboardText, keyboardMarkup)
}

func HandleInlineCallbackQuery(callbackQuery *TelegramQuery) {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC CALLBACK] %v", rec)
		}
	}()
	chatID := callbackQuery.Message.Chat.ID
	messageID := callbackQuery.Message.MessageID

	answerEndpoint := fmt.Sprintf("https://api.telegram.org/bot%s/answerCallbackQuery", telegramBotToken)
	standardHTTPClient.PostForm(answerEndpoint, map[string][]string{"callback_query_id": {callbackQuery.ID}})

	switch callbackQuery.Data {
	case "menu":
		DeleteTelegramMessage(chatID, messageID)
		RenderMainMenu(chatID)
	case "usernames":
		DeleteTelegramMessage(chatID, messageID)
		appStats.mutex.Lock()
		appStats.pendingAction = "usernames"
		appStats.mutex.Unlock()
		PromptForUsernamesFile(chatID)
	case "proxies":
		DeleteTelegramMessage(chatID, messageID)
		appStats.mutex.Lock()
		appStats.pendingAction = "proxies"
		appStats.mutex.Unlock()
		PromptForProxiesFile(chatID)
	case "threads":
		DeleteTelegramMessage(chatID, messageID)
		appStats.mutex.Lock()
		appStats.pendingAction = "threads"
		appStats.mutex.Unlock()
		PromptForThreadsCount(chatID)
	case "user_cap":
		DeleteTelegramMessage(chatID, messageID)
		appStats.mutex.Lock()
		appStats.pendingAction = "user_cap"
		appStats.mutex.Unlock()
		PromptForUserCap(chatID)
	case "start":
		StartCheckerLoopExecution(chatID)
	case "stop":
		StopCheckerLoopExecution(chatID)
	}
}

func PromptForProxiesFile(chatID int64) {
	text := "<b>Send the proxies file (.txt)</b>\n\nEach line: <code>IP:PORT</code>"
	markup := map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": "Cancel", "callback_data": "menu"}},
		},
	}
	SendTelegramMessage(chatID, text, markup)
}

func PromptForUsernamesFile(chatID int64) {
	text := "<b>Send the usernames file (.txt)</b>\n\nEach line: one username"
	markup := map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": "Cancel", "callback_data": "menu"}},
		},
	}
	SendTelegramMessage(chatID, text, markup)
}

func PromptForThreadsCount(chatID int64) {
	text := fmt.Sprintf("<b>Send the number of Threads (Pool Size)</b>\n\nCurrent: <code>%d</code>", atomic.LoadInt32(&configuredPoolSize))
	markup := map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": "Cancel", "callback_data": "menu"}},
		},
	}
	SendTelegramMessage(chatID, text, markup)
}

func PromptForUserCap(chatID int64) {
	text := fmt.Sprintf("<b>Send the Per-User Cap</b>\n\nCurrent: <code>%d</code>", atomic.LoadInt32(&configuredPerUserCap))
	markup := map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": "Cancel", "callback_data": "menu"}},
		},
	}
	SendTelegramMessage(chatID, text, markup)
}

func ProcessUploadedProxiesFile(chatID int64, fileID string, fileName string) {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC PROXIES FILE] %v", rec)
		}
	}()
	if !strings.HasSuffix(strings.ToLower(fileName), ".txt") {
		SendTelegramMessage(chatID, "<b>Error:</b> File must be .txt", nil)
		return
	}
	tempPath, err := DownloadTelegramDocument(fileID)
	if err != nil {
		SendTelegramMessage(chatID, "<b>Download failed</b>\n"+err.Error(), nil)
		return
	}
	os.Remove(proxiesFilePath)
	if err := os.Rename(tempPath, proxiesFilePath); err != nil {
		fileData, readErr := os.ReadFile(tempPath)
		if readErr != nil {
			SendTelegramMessage(chatID, "<b>Save failed</b>", nil)
			return
		}
		os.WriteFile(proxiesFilePath, fileData, 0644)
		os.Remove(tempPath)
	}
	proxyCount := LoadProxiesFromFileIntoMemory()
	text := fmt.Sprintf("<b>Proxies updated</b>\n\n<b>Count:</b> %d", proxyCount)
	markup := map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": "Back to Menu", "callback_data": "menu"}},
		},
	}
	SendTelegramMessage(chatID, text, markup)
}

func ProcessUploadedUsernamesFile(chatID int64, fileID string, fileName string) {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC USERS FILE] %v", rec)
		}
	}()
	if !strings.HasSuffix(strings.ToLower(fileName), ".txt") {
		SendTelegramMessage(chatID, "<b>Error:</b> File must be .txt", nil)
		return
	}
	tempPath, err := DownloadTelegramDocument(fileID)
	if err != nil {
		SendTelegramMessage(chatID, "<b>Download failed</b>\n"+err.Error(), nil)
		return
	}
	os.Remove(usernamesFilePath)
	if err := os.Rename(tempPath, usernamesFilePath); err != nil {
		fileData, readErr := os.ReadFile(tempPath)
		if readErr != nil {
			SendTelegramMessage(chatID, "<b>Save failed</b>", nil)
			return
		}
		os.WriteFile(usernamesFilePath, fileData, 0644)
		os.Remove(tempPath)
	}
	RemoveDuplicateUsernamesInFile(usernamesFilePath)
	userCount := LoadUsernamesFromFileIntoMemory()
	text := fmt.Sprintf("<b>Usernames saved</b>\n\n<b>Unique:</b> %d", userCount)
	markup := map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": "Back to Menu", "callback_data": "menu"}},
		},
	}
	SendTelegramMessage(chatID, text, markup)
}

func DownloadTelegramDocument(fileID string) (string, error) {
	metaEndpoint := fmt.Sprintf("https://api.telegram.org/bot%s/getFile?file_id=%s", telegramBotToken, fileID)
	resp, err := standardHTTPClient.Get(metaEndpoint)
	if err != nil {
		return "", fmt.Errorf("connection error: %v", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	var responseParsed struct {
		OK     bool `json:"ok"`
		Result struct {
			FilePath string `json:"file_path"`
		} `json:"result"`
	}
	if err := json.Unmarshal(bodyBytes, &responseParsed); err != nil {
		return "", fmt.Errorf("json parse error: %v", err)
	}
	if !responseParsed.OK || responseParsed.Result.FilePath == "" {
		return "", fmt.Errorf("invalid file path")
	}

	downloadURL := fmt.Sprintf("https://api.telegram.org/file/bot%s/%s", telegramBotToken, responseParsed.Result.FilePath)
	downloadResp, err := standardHTTPClient.Get(downloadURL)
	if err != nil {
		return "", fmt.Errorf("download error: %v", err)
	}
	defer downloadResp.Body.Close()

	os.MkdirAll(tempDirectory, 0755)
	localTempPath := fmt.Sprintf("%s/%s", tempDirectory, fileID)
	outputFile, err := os.Create(localTempPath)
	if err != nil {
		return "", fmt.Errorf("create file error: %v", err)
	}
	defer outputFile.Close()

	if _, err := io.Copy(outputFile, downloadResp.Body); err != nil {
		return "", fmt.Errorf("write error: %v", err)
	}
	return localTempPath, nil
}

func StartCheckerLoopExecution(chatID int64) {
	if !atomic.CompareAndSwapInt32(&isLoopRunning, 0, 1) {
		return
	}
	appStats.mutex.RLock()
	hasUsers := len(appStats.loadedUsernames) > 0
	hasProxies := len(appStats.loadedProxies) > 0
	appStats.mutex.RUnlock()

	if !hasUsers {
		atomic.StoreInt32(&isLoopRunning, 0)
		SendTelegramMessage(chatID, "<b>No usernames loaded!</b>", nil)
		return
	}
	if !hasProxies {
		atomic.StoreInt32(&isLoopRunning, 0)
		SendTelegramMessage(chatID, "<b>No proxies loaded!</b>", nil)
		return
	}
	WriteLogEntry("[START] Continuous scanning loop initiated")
	go RunContinuousBackgroundLoop(chatID)
}

func StopCheckerLoopExecution(chatID int64) {
	if !atomic.CompareAndSwapInt32(&isLoopRunning, 1, 0) {
		return
	}
	WriteLogEntry("[STOP] Loop manually stopped by administrator")
	scannerMutex.Lock()
	engineRef := activeScanner
	scannerMutex.Unlock()
	if engineRef != nil {
		engineRef.StopEngine()
	}
	SendTelegramMessage(chatID, "<b>Scanner loop stopped successfully.</b>", nil)
}

func CalculateRequestsPerSecondTick() {
	nowNano := time.Now().UnixNano()
	currentSent := atomic.LoadInt64(&appStats.totalRequestsSent)
	previousSample := atomic.SwapInt64(&appStats.lastSampleCount, currentSent)
	previousResponseSample := atomic.LoadInt64(&appStats.lastResponseSample)
	currentResponses := atomic.LoadInt64(&appStats.totalResponses)
	atomic.StoreInt64(&appStats.lastResponseSample, currentResponses)

	previousNano := atomic.SwapInt64(&appStats.lastSampleNano, nowNano)
	if previousNano == 0 {
		previousNano = atomic.LoadInt64(&appStats.startTimeNano)
	}
	intervalSeconds := float64(nowNano-previousNano) / float64(time.Second)
	if intervalSeconds > 0 {
		atomic.StoreInt64(&appStats.currentRPS, int64(float64(currentSent-previousSample)/intervalSeconds))
		atomic.StoreInt64(&appStats.responseRPS, int64(float64(currentResponses-previousResponseSample)/intervalSeconds))
	}

	startNano := atomic.LoadInt64(&appStats.startTimeNano)
	if startNano == 0 {
		return
	}
	elapsedSeconds := time.Since(time.Unix(0, startNano)).Seconds()
	if elapsedSeconds <= 0 {
		return
	}
	atomic.StoreInt64(&appStats.averageRPS, int64(float64(currentSent)/elapsedSeconds))
	atomic.StoreInt64(&appStats.averageResponseRPS, int64(float64(currentResponses)/elapsedSeconds))
}

func StartPerformanceMetricsCollector() {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC RPS COLLECTOR] %v", rec)
		}
	}()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			CalculateRequestsPerSecondTick()
			if atomic.LoadInt32(&isLoopRunning) == 0 {
				CalculateRequestsPerSecondTick()
				return
			}
			appStats.mutex.RLock()
			isActive := appStats.isRunning
			appStats.mutex.RUnlock()
			if !isActive {
				CalculateRequestsPerSecondTick()
				return
			}
		case <-appCtx.Done():
			return
		}
	}
}

func StartSystemResourceMonitor() {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC SYS MONITOR] %v", rec)
		}
	}()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		allocMB := float64(memStats.Alloc) / (1024 * 1024)

		rssMB, cpuPct := getPlatformSpecificMetrics()

		appStats.mutex.Lock()
		appStats.memoryUsageMB = allocMB
		appStats.rssMemoryMB = rssMB
		appStats.cpuUsagePercent = cpuPct
		appStats.activeGoroutines = runtime.NumGoroutine()
		isRunning := appStats.isRunning
		appStats.mutex.Unlock()

		select {
		case <-ticker.C:
		case <-appCtx.Done():
			return
		}
	}
}

func StartTelegramUpdateListener() {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC TG LISTENER] %v", rec)
		}
	}()
	offsetID := 0
	SendTelegramMessage(ownerTelegramID, "<b>Jekr Need Started</b>\n\nSend /start\n\n<i>Instant alerts arrive via Discord</i>", nil)
	WriteLogEntry("[CONTROL] Telegram listener initialized")

	for {
		select {
		case <-appCtx.Done():
			WriteLogEntry("[CONTROL] Telegram listener stopped gracefully.")
			return
		default:
		}

		updatesURL := fmt.Sprintf("https://api.telegram.org/bot%s/getUpdates?offset=%d&timeout=25", telegramBotToken, offsetID)

		req, err := http.NewRequestWithContext(appCtx, http.MethodGet, updatesURL, nil)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		httpResp, err := standardHTTPClient.Do(req)
		if err != nil {
			if appCtx.Err() != nil {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				WriteLogEntry("[CONTROL] Telegram long-poll timeout; retrying...")
			} else {
				WriteLogEntry("[CONTROL ERROR] Failed to fetch telegram updates: %v", err)
			}
			time.Sleep(2 * time.Second)
			continue
		}
		responseBody, _ := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()

		var parsedUpdates struct {
			OK     bool             `json:"ok"`
			Result []TelegramUpdate `json:"result"`
		}
		if err := json.Unmarshal(responseBody, &parsedUpdates); err != nil {
			time.Sleep(time.Second)
			continue
		}

		for _, update := range parsedUpdates.Result {
			offsetID = update.UpdateID + 1
			if update.IncomingMsg != nil && update.IncomingMsg.Chat.ID == ownerTelegramID {
				HandleIncomingTelegramMessage(update.IncomingMsg)
			}
			if update.CallbackQuery != nil && update.CallbackQuery.Sender.ID == ownerTelegramID {
				HandleInlineCallbackQuery(update.CallbackQuery)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func HandleIncomingTelegramMessage(msg *TelegramMessage) {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC INCOMING MSG] %v", rec)
		}
	}()
	chatID := msg.Chat.ID
	if msg.Document != nil {
		appStats.mutex.RLock()
		pendingState := appStats.pendingAction
		appStats.mutex.RUnlock()

		if pendingState == "proxies" {
			WriteLogEntry("[CONTROL] Received proxies file: %s", msg.Document.FileName)
			ProcessUploadedProxiesFile(chatID, msg.Document.FileID, msg.Document.FileName)
			appStats.mutex.Lock()
			appStats.pendingAction = ""
			appStats.mutex.Unlock()
		} else if pendingState == "usernames" {
			WriteLogEntry("[CONTROL] Received usernames file: %s", msg.Document.FileName)
			ProcessUploadedUsernamesFile(chatID, msg.Document.FileID, msg.Document.FileName)
			appStats.mutex.Lock()
			appStats.pendingAction = ""
			appStats.mutex.Unlock()
		} else {
			SendTelegramMessage(chatID, "Not waiting for any file currently.", nil)
		}
		return
	}

	textInput := strings.TrimSpace(msg.Text)
	WriteLogEntry("[CONTROL] Message received: %s", textInput)

	appStats.mutex.RLock()
	pendingState := appStats.pendingAction
	appStats.mutex.RUnlock()

	if pendingState == "threads" {
		val, err := strconv.Atoi(textInput)
		if err != nil || val <= 0 {
			SendTelegramMessage(chatID, "<b>Error:</b> Please send a valid positive number for Threads.", nil)
			return
		}
		atomic.StoreInt32(&configuredPoolSize, int32(val))
		appStats.mutex.Lock()
		appStats.pendingAction = ""
		appStats.mutex.Unlock()
		WriteLogEntry("[THREADS UPDATED] %d", val)

		markup := map[string]interface{}{
			"inline_keyboard": [][]map[string]string{
				{{"text": "Back to Menu", "callback_data": "menu"}},
			},
		}
		SendTelegramMessage(chatID, fmt.Sprintf("<b>Threads updated</b>\n\n<b>New Pool Size:</b> %d", val), markup)
		return
	} else if pendingState == "user_cap" {
		val, err := strconv.Atoi(textInput)
		if err != nil || val <= 0 {
			SendTelegramMessage(chatID, "<b>Error:</b> Please send a valid positive number for Per-User Cap.", nil)
			return
		}
		atomic.StoreInt32(&configuredPerUserCap, int32(val))
		appStats.mutex.Lock()
		appStats.pendingAction = ""
		appStats.mutex.Unlock()
		WriteLogEntry("[PER-USER CAP UPDATED] %d", val)

		markup := map[string]interface{}{
			"inline_keyboard": [][]map[string]string{
				{{"text": "Back to Menu", "callback_data": "menu"}},
			},
		}
		SendTelegramMessage(chatID, fmt.Sprintf("<b>Per-User Cap updated</b>\n\n<b>New Cap:</b> %d", val), markup)
		return
	}

	switch {
	case textInput == "/start" || textInput == "/menu":
		RenderMainMenu(chatID)
	case textInput == "/stop":
		StopCheckerLoopExecution(chatID)
	}
}

func RunContinuousBackgroundLoop(chatID int64) {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC BACKGROUND LOOP] %v", rec)
		}
	}()
	for {
		if atomic.LoadInt32(&isLoopRunning) == 0 {
			WriteLogEntry("[LOOP EXIT] Scanning loop terminated.")
			return
		}
		ExecuteSingleScanCycle(chatID)
		if atomic.LoadInt32(&isLoopRunning) == 0 {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

func ExecuteSingleScanCycle(chatID int64) {
	defer func() {
		if rec := recover(); rec != nil {
			WriteLogEntry("[PANIC SCAN CYCLE] %v", rec)
		}
	}()
	cycleMutex.Lock()
	currentCycleHits = make(map[string]struct{})
	usernameFirstSeen = make(map[string]time.Time)
	usernameCheckTimes = make(map[string][2]time.Time)
	cycleMutex.Unlock()

	os.MkdirAll("data", 0755)

	LoadProxiesFromFileIntoMemory()
	LoadUsernamesFromFileIntoMemory()

	appStats.mutex.Lock()
	usernamesCopy := make([]string, len(appStats.loadedUsernames))
	copy(usernamesCopy, appStats.loadedUsernames)
	proxiesCount := len(appStats.loadedProxies)
	appStats.totalUsernames = len(usernamesCopy)
	appStats.totalProxiesCount = proxiesCount
	appStats.isRunning = true
	appStats.mutex.Unlock()

	if len(usernamesCopy) == 0 || proxiesCount == 0 {
		WriteLogEntry("[CYCLE] No data found (usernames=%d, proxies=%d)", len(usernamesCopy), proxiesCount)
		appStats.mutex.Lock()
		appStats.isRunning = false
		appStats.mutex.Unlock()
		time.Sleep(5 * time.Second)
		return
	}

	atomic.StoreInt64(&appStats.completedTasks, 0)
	atomic.StoreInt64(&appStats.availableFound, 0)
	atomic.StoreInt64(&appStats.retryCount, 0)
	atomic.StoreInt64(&appStats.totalRequestsSent, 0)
	atomic.StoreInt64(&appStats.totalResponses, 0)
	atomic.StoreInt64(&appStats.requestErrors, 0)
	atomic.StoreInt64(&appStats.timeoutErrors, 0)
	atomic.StoreInt64(&appStats.cancelErrors, 0)
	atomic.StoreInt64(&appStats.networkErrors, 0)
	atomic.StoreInt64(&appStats.tlsErrors, 0)
	atomic.StoreInt64(&appStats.httpErrors, 0)
	atomic.StoreInt64(&appStats.bodyLargeErrors, 0)
	atomic.StoreInt64(&appStats.unknownErrors, 0)
	atomic.StoreInt64(&appStats.spamCount, 0)
	atomic.StoreInt64(&appStats.currentRPS, 0)
	atomic.StoreInt64(&appStats.averageRPS, 0)
	atomic.StoreInt64(&appStats.responseRPS, 0)
	atomic.StoreInt64(&appStats.averageResponseRPS, 0)
	atomic.StoreInt64(&appStats.lastSampleCount, 0)
	atomic.StoreInt64(&appStats.lastResponseSample, 0)
	startNano := time.Now().UnixNano()
	atomic.StoreInt64(&appStats.startTimeNano, startNano)
	atomic.StoreInt64(&appStats.lastSampleNano, startNano)

	nowBaghdad := time.Now().In(baghdadTimezone)
	WriteLogEntry("[CYCLE START] %s | users=%d proxies=%d threads=%d cap=%d",
		nowBaghdad.Format("03:04:05 PM"), len(usernamesCopy), proxiesCount, atomic.LoadInt32(&configuredPoolSize), atomic.LoadInt32(&configuredPerUserCap))

	discordStreamDoneChannel := make(chan struct{})

	resultsChannel = make(chan ScanResult, 10000)
	go StartResultsWriterAndNotifier()

	cycleCtx, cycleCancel := context.WithCancel(appCtx)
	defer cycleCancel()

	scannerEngineInstance := InitializeScannerEngine(cycleCtx, usernamesCopy)

	scannerMutex.Lock()
	activeScanner = scannerEngineInstance
	scannerMutex.Unlock()

	go StartDiscordLiveStatusStream(discordStreamDoneChannel)
	go StartPerformanceMetricsCollector()

	scannerEngineInstance.StartScanning()

	close(discordStreamDoneChannel)
	close(resultsChannel)
	<-resultsWorkerDone

	cycleEndTime := time.Now()
	cycleMutex.Lock()
	for foundUser := range currentCycleHits {
		seenUsernames[foundUser] = struct{}{}
	}
	previousCycleEnd = cycleEndTime
	cycleMutex.Unlock()

	SaveCheckerState()

	appStats.mutex.Lock()
	appStats.isRunning = false
	appStats.mutex.Unlock()

	scannerMutex.Lock()
	if activeScanner == scannerEngineInstance {
		activeScanner = nil
	}
	scannerMutex.Unlock()

	WriteLogEntry("[CYCLE END] %s", cycleEndTime.In(baghdadTimezone).Format("2006-01-02 03:04:05 PM"))
}

func main() {
	rand.Seed(time.Now().UnixNano())

	if telegramBotToken == "" || ownerTelegramID == 0 {
		fmt.Println("Missing TELEGRAM_BOT_TOKEN or OWNER_TELEGRAM_ID environment variable")
		return
	}
	if os.Getenv("DISCORD_WEBHOOK_URL") == "" {
		fmt.Println("DISCORD_WEBHOOK_URL is not set; Discord notifications are disabled")
	}

	fmt.Println("============================================================")
	fmt.Println("                Jekr Need (uTLS + fasthttp)                 ")
	fmt.Println("============================================================")

	os.MkdirAll("data", 0755)
	os.MkdirAll(tempDirectory, 0755)

	if err := InitializeLogFiles(); err != nil {
		fmt.Println("Failed to initialize log files:", err)
	}
	defer func() {
		if logFileHandle != nil {
			logFileHandle.Close()
		}
		if debugLogHandle != nil {
			debugLogHandle.Close()
		}
	}()

	WriteLogEntry("[MAIN] Starting Jekr Need engine...")
	RemoveDuplicateUsernamesInFile(usernamesFilePath)
	LoadSeenUsernamesDatabase()
	LoadCheckerState()

	proxyCount := LoadProxiesFromFileIntoMemory()
	WriteLogEntry("[MAIN] Loaded proxies: %d", proxyCount)

	userCount := LoadUsernamesFromFileIntoMemory()
	WriteLogEntry("[MAIN] Loaded usernames: %d", userCount)

	WriteLogEntry("[MAIN] Configured threads: %d | Per-user cap: %d", atomic.LoadInt32(&configuredPoolSize), atomic.LoadInt32(&configuredPerUserCap))
	WriteLogEntry("[MAIN] Engine ready. Launching background monitors and telegram listener...")

	go StartTelegramUpdateListener()
	go StartSystemResourceMonitor()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	WriteLogEntry("[SHUTDOWN] Signal received. Shutting down application gracefully...")
	appCancel()
	time.Sleep(1 * time.Second)
	WriteLogEntry("[SHUTDOWN] Application exited successfully.")
}
