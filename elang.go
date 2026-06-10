package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Result struct {
	StatusCode int
	Duration   time.Duration
	Err        error
}

func main() {
	// --- INPUT MANUAL URL TARGET ---
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Masukkan URL Target (contoh: http://localhost:8080 atau https://google.com): ")
	targetURL, _ := reader.ReadString('\n')
	targetURL = strings.TrimSpace(targetURL) // Menghapus karakter enter/spasi di ujung

	// Validasi input sederhana
	if targetURL == "" {
		fmt.Println("Error: URL tidak boleh kosong!")
		return
	}

	// --- KONFIGURASI BEBAN ---
	totalRequests := 100
	concurrency := 10
	// ---------------------------

	fmt.Printf("\nMemulai stress test ke: %s\n", targetURL)
	fmt.Printf("Total Request: %d | Concurrency (Workers): %d\n\n", totalRequests, concurrency)

	startTime := time.Now()

	jobs := make(chan int, totalRequests)
	results := make(chan Result, totalRequests)

	var wg sync.WaitGroup
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	for w := 1; w <= concurrency; w++ {
		wg.Add(1)
		go worker(targetURL, client, jobs, results, &wg)
	}

	for j := 1; j <= totalRequests; j++ {
		jobs <- j
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	successCount := 0
	failCount := 0
	statusCodes := make(map[int]int)
	
	// Map untuk mencatat jenis error unik agar terminal tidak penuh jika semua error sama
	errorLog := make(map[string]int)

	for res := range results {
		if res.Err != nil {
			failCount++
			// Catat pesan errornya
			errorLog[res.Err.Error()]++
		} else {
			successCount++
			statusCodes[res.StatusCode]++
		}
	}

	totalDuration := time.Since(startTime)

	// --- OUTPUT STATISTIK ---
	fmt.Println("\n================ STATISTIK ================")
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
		fmt.Println("Log Error Terjadi   :")
		for errMsg, count := range errorLog {
			fmt.Printf("  - [ %d kali ] %s\n", count, errMsg)
		}
	}
	fmt.Println("===========================================")
}

func worker(url string, client *http.Client, jobs <-chan int, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for range jobs {
		reqStart := time.Now()
		resp, err := client.Get(url)
		duration := time.Since(reqStart)

		if err != nil {
			results <- Result{Err: err, Duration: duration}
			continue
		}

		resp.Body.Close()
		results <- Result{StatusCode: resp.StatusCode, Duration: duration}
	}
}

