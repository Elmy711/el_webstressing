package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Result struct {
	WorkerID   int
	StatusCode int
	Duration   time.Duration
	Err        error
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("=====================================================")
	fmt.Println("        ADVANCED ELANG HTTP LOAD TESTER            ")
	fmt.Println("=====================================================")

	// 1. Input URL Target
	fmt.Print("1. URL Target : ")
	targetURL, _ := reader.ReadString('\n')
	targetURL = strings.TrimSpace(targetURL)
	if targetURL == "" {
		fmt.Println("Error: URL tidak boleh kosong!")
		return
	}

	// 2. Input HTTP Method
	fmt.Print("2. HTTP Method (GET, POST, PUT, DELETE) : ")
	method, _ := reader.ReadString('\n')
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		method = "GET"
	}

	// 3. Input HTTP Body (Hanya jika POST/PUT)
	var bodyData []byte
	if method == "POST" || method == "PUT" {
		fmt.Print("   -> Payload Body (JSON) [Kosongkan jika tidak ada]: ")
		bodyStr, _ := reader.ReadString('\n')
		bodyData = []byte(strings.TrimSpace(bodyStr))
	}

	// 4. Input Durasi Pengujian
	fmt.Print("3. Durasi : ")
	durationStr, _ := reader.ReadString('\n')
	durationSec, err := strconv.Atoi(strings.TrimSpace(durationStr))
	if err != nil || durationSec <= 0 {
		fmt.Println("Error: Durasi harus berupa angka bulat positif!")
		return
	}

	// 5. Input Concurrency (Workers)
	fmt.Print("4. Workers : ")
	workerStr, _ := reader.ReadString('\n')
	concurrency, err := strconv.Atoi(strings.TrimSpace(workerStr))
	if err != nil || concurrency <= 0 {
		fmt.Println("Error: Jumlah workers harus berupa angka bulat positif!")
		return
	}

	// 6. Input Rate Limit (RPS)
	fmt.Print("5. RPS [0 untuk tanpa batas]: ")
	rpsStr, _ := reader.ReadString('\n')
	maxRPS, err := strconv.Atoi(strings.TrimSpace(rpsStr))
	if err != nil || maxRPS < 0 {
		fmt.Println("Error: RPS harus berupa angka positif atau 0!")
		return
	}

	// 7. Input Mode Debug
	fmt.Print("6. Mode Debug? (y/n): ")
	debugStr, _ := reader.ReadString('\n')
	debugStr = strings.ToLower(strings.TrimSpace(debugStr))
	debugMode := debugStr == "y" || debugStr == "yes"

	// Ringkasan Konfigurasi
	fmt.Println("\n=================== KONFIGURASI =====================")
	fmt.Printf("Target    : %s (%s)\n", targetURL, method)
	fmt.Printf("Durasi   : %d detik\n", durationSec)
	fmt.Printf("Workers  : %d\n", concurrency)
	if maxRPS > 0 {
		fmt.Printf("Rate Limit   : %d RPS\n", maxRPS)
	} else {
		fmt.Println("Rate Limit   : Unlimited (Hantam Penuh)")
	}
	fmt.Printf("Mode Debug   : %t\n", debugMode)
	fmt.Println("=====================================================")
	fmt.Println("Mulai .... Tekan Ctrl+C untuk membatalkan.\n")

	testDuration := time.Duration(durationSec) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	results := make(chan Result, 50000) // Buffer diperbesar untuk mengantisipasi ribuan request instant
	var wg sync.WaitGroup

	// Perbaikan: Kustomisasi Transport untuk memaksa pembersihan koneksi lama
	transport := &http.Transport{
		MaxIdleConns:        concurrency,
		MaxIdleConnsPerHost: concurrency,
		IdleConnTimeout:     30 * time.Second,
		DisableKeepAlives:   maxRPS == 0, // Jika mode brutal/unlimited, matikan keep-alive agar port tidak macet
	}

	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: transport,
	}

	var ticker *time.Ticker
	if maxRPS > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(maxRPS))
		defer ticker.Stop()
	}

	startTime := time.Now()

	// Memulai Worker Pool
	for w := 1; w <= concurrency; w++ {
		wg.Add(1)
		go worker(ctx, w, method, targetURL, bodyData, client, ticker, results, &wg)
	}

	// Menunggu seluruh worker selesai
	go func() {
		wg.Wait()
		close(results)
	}()

	var durations []time.Duration
	successCount := 0
	failCount := 0
	statusCodes := make(map[int]int)
	errorLog := make(map[string]int)

	for res := range results {
		if res.Err != nil {
			failCount++
			errorLog[res.Err.Error()]++
			if debugMode {
				fmt.Printf(" 📕 #%d -> GAGAL : %v\n", res.WorkerID, res.Err)
			}
		} else {
			successCount++
			durations = append(durations, res.Duration)
			statusCodes[res.StatusCode]++
			if debugMode {
				fmt.Printf(" 📗 #%d -> SUKSES : %d [%v]\n", res.WorkerID, res.StatusCode, res.Duration)
			}
		}
	}

	actualDuration := time.Since(startTime)

	// --- KALKULASI STATISTIK LANJUTAN ---
	var p50, p95, p99 time.Duration
	var avgDuration time.Duration
	totalRequests := successCount + failCount

	if len(durations) > 0 {
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

		var totalTime time.Duration
		for _, d := range durations {
			totalTime += d
		}
		avgDuration = totalTime / time.Duration(len(durations))

		p50 = durations[len(durations)*50/100]
		p95 = durations[len(durations)*95/100]
		p99 = durations[len(durations)*99/100]
	}

	// --- OUTPUT HASIL AKHIR ---
	fmt.Println("\n==================== STATISTIK ====================")
	fmt.Printf("Total Waktu Berjalan : %v\n", actualDuration)
	fmt.Printf("Total Requests Sent  : %d\n", totalRequests)
	fmt.Printf("Requests Sukses      : %d\n", successCount)
	fmt.Printf("Requests Gagal       : %d\n", failCount)

	if totalRequests > 0 && actualDuration.Seconds() > 0 {
		fmt.Printf("Rata-rata RPS Aktual : %.2f req/sec\n", float64(totalRequests)/actualDuration.Seconds())
	}

	if len(durations) > 0 {
		fmt.Println("\nAnalisis Latensi (Hanya Request Sukses):")
		fmt.Printf("  - Rata-rata (Avg)  : %v\n", avgDuration)
		fmt.Printf("  - p50 (Median)     : %v\n", p50)
		fmt.Printf("  - p95 (95%% User)   : %v\n", p95)
		fmt.Printf("  - p99 (99%% User)   : %v (Terlambat)\n", p99)
	}

	if len(statusCodes) > 0 {
		fmt.Println("\nStatus Code   :")
		for code, count := range statusCodes {
			fmt.Printf("  - Status %d : %d kali\n", code, count)
		}
	}

	if len(errorLog) > 0 {
		fmt.Println("\nRincian Error :")
		for errMsg, count := range errorLog {
			fmt.Printf("  - [ %d kali ] %s\n", count, errMsg)
		}
	}
	fmt.Println("=====================================================")
}

func worker(ctx context.Context, id int, method, url string, body []byte, client *http.Client, ticker *time.Ticker, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if ticker != nil {
				<-ticker.C
			}

			// Perbaikan krusial: Memastikan request dibentuk bersih menggunakan variabel 'url' lokal dari parameter fungsi
			req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(body))
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				results <- Result{WorkerID: id, Err: err}
				continue
			}

			// Headers Standar Berorientasi Browser
			req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
			req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
			req.Header.Set("Accept-Language", "en-US,en;q=0.5")
			
			// Perbaikan Logika: Header Content-Type JSON HANYA dipasang jika method-nya POST/PUT dan body terisi
			if (method == "POST" || method == "PUT") && len(body) > 0 {
				req.Header.Set("Content-Type", "application/json")
			}

			reqStart := time.Now()
			resp, err := client.Do(req)
			duration := time.Since(reqStart)

			if err != nil {
				if ctx.Err() != nil {
					return
				}
				results <- Result{WorkerID: id, Err: err, Duration: duration}
				continue
			}

			resp.Body.Close()

			results <- Result{
				WorkerID:   id,
				StatusCode: resp.StatusCode,
				Duration:   duration,
			}
		}
	}
}
