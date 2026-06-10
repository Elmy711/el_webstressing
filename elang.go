package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Result struct {
	WorkerID   int
	JobID      int
	StatusCode int
	Duration   time.Duration
	Err        error
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	// 1. Input URL Target
	fmt.Print("1. Masukkan URL Target (contoh: http://localhost:8080): ")
	targetURL, _ := reader.ReadString('\n')
	targetURL = strings.TrimSpace(targetURL)
	if targetURL == "" {
		fmt.Println("Error: URL tidak boleh kosong!")
		return
	}

	// 2. Input Total Request
	fmt.Print("2. Masukkan Total Request (contoh: 100): ")
	totalStr, _ := reader.ReadString('\n')
	totalRequests, err := strconv.Atoi(strings.TrimSpace(totalStr))
	if err != nil || totalRequests <= 0 {
		fmt.Println("Error: Total request harus berupa angka bulat positif!")
		return
	}

	// 3. Input Concurrency / Workers
	fmt.Print("3. Masukkan Jumlah Workers/Concurrency (contoh: 10): ")
	workerStr, _ := reader.ReadString('\n')
	concurrency, err := strconv.Atoi(strings.TrimSpace(workerStr))
	if err != nil || concurrency <= 0 {
		fmt.Println("Error: Jumlah workers harus berupa angka bulat positif!")
		return
	}

	// 4. Input Mode Debug
	fmt.Print("4. Aktifkan Mode Debug? (y/n): ")
	debugStr, _ := reader.ReadString('\n')
	debugStr = strings.ToLower(strings.TrimSpace(debugStr))
	debugMode := debugStr == "y" || debugStr == "yes"

	// Ringkasan Konfigurasi
	fmt.Println("\n================ KONFIGURASI ================")
	fmt.Printf("Target URL   : %s\n", targetURL)
	fmt.Printf("Total Request: %d\n", totalRequests)
	fmt.Printf("Workers Pool : %d\n", concurrency)
	fmt.Printf("Mode Debug   : %t\n", debugMode)
	fmt.Println("=============================================")
	fmt.Println("Memulai pengujian... Tekan Ctrl+C untuk membatalkan.\n")

	startTime := time.Now()

	jobs := make(chan int, totalRequests)
	results := make(chan Result, totalRequests)

	var wg sync.WaitGroup
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// Membuat Worker Pool
	for w := 1; w <= concurrency; w++ {
		wg.Add(1)
		go worker(w, targetURL, client, jobs, results, &wg)
	}

	// Mendistribusikan Job
	for j := 1; j <= totalRequests; j++ {
		jobs <- j
	}
	close(jobs)

	// Menunggu seluruh worker selesai di background
	go func() {
		wg.Wait()
		close(results)
	}()

	successCount := 0
	failCount := 0
	statusCodes := make(map[int]int)
	errorLog := make(map[string]int)

	// Membaca hasil (sambil menampilkan debug jika aktif)
	for res := range results {
		if res.Err != nil {
			failCount++
			errorLog[res.Err.Error()]++
			if debugMode {
				fmt.Printf("[DEBUG] Worker #%d | Job #%d -> GAGAL: %v\n", res.WorkerID, res.JobID, res.Err)
			}
		} else {
			successCount++
			statusCodes[res.StatusCode]++
			if debugMode {
				fmt.Printf("[DEBUG] Worker #%d | Job #%d -> SUKSES (Status: %d) [%v]\n", res.WorkerID, res.JobID, res.StatusCode, res.Duration)
			}
		}
	}

	totalDuration := time.Since(startTime)

	// --- OUTPUT STATISTIK ---
	fmt.Println("\n================ HASIL AKHIR ================")
	fmt.Printf("Waktu Pengujian     : %v\n", totalDuration)
	fmt.Printf("Request Sukses      : %d\n", successCount)
	fmt.Printf("Request Gagal/Error : %d\n", failCount)

	if len(statusCodes) > 0 {
		fmt.Println("Detail Status Code  :")
		for code, count := range statusCodes {
			fmt.Printf("  - Status %d: %d kali\n", code, count)
		}
	}

	if len(errorLog) > 0 {
		fmt.Println("Rincian Error       :")
		for errMsg, count := range errorLog {
			fmt.Printf("  - [ %d kali ] %s\n", count, errMsg)
		}
	}
	fmt.Println("=============================================")
}

func worker(id int, url string, client *http.Client, jobs <-chan int, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for jobID := range jobs {
		reqStart := time.Now()
		resp, err := client.Get(url)
		duration := time.Since(reqStart)

		if err != nil {
			results <- Result{WorkerID: id, JobID: jobID, Err: err, Duration: duration}
			continue
		}

		resp.Body.Close()
		results <- Result{WorkerID: id, JobID: jobID, StatusCode: resp.StatusCode, Duration: duration}
	}
}
