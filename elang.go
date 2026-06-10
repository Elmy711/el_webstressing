package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Struktur untuk menyimpan hasil dari setiap request
type Result struct {
	StatusCode int
	Duration   time.Duration
	Err        error
}

func main() {
	// --- KONFIGURASI TARGET ---
	targetURL := "http://localhost:8080" // Ganti dengan URL target Anda
	totalRequests := 100                 // Jumlah total request yang akan dikirim
	concurrency := 10                   // Jumlah worker (goroutine) yang berjalan bersamaan
	// ---------------------------

	fmt.Printf("Memulai stress test ke: %s\n", targetURL)
	fmt.Printf("Total Request: %d | Concurrency (Workers): %d\n\n", totalRequests, concurrency)

	startTime := time.Now()

	// Channel untuk mendistribusikan tugas dan menampung hasil
	jobs := make(chan int, totalRequests)
	results := make(chan Result, totalRequests)

	var wg sync.WaitGroup
	client := &http.Client{
		Timeout: 5 * time.Second, // Timeout agar tidak menggantung jika server down
	}

	// 1. Membuat Worker Pool
	for w := 1; w <= concurrency; w++ {
		wg.Add(1)
		go worker(targetURL, client, jobs, results, &wg)
	}

	// 2. Mengirimkan daftar tugas ke channel jobs
	for j := 1; j <= totalRequests; j++ {
		jobs <- j
	}
	close(jobs) // Menandakan tidak ada tugas baru lagi

	// 3. Menunggu semua worker selesai di goroutine terpisah
	go func() {
		wg.Wait()
		close(results) // Tutup channel hasil setelah semua worker selesai
	}()

	// 4. Memproses dan menghitung statistik hasil
	successCount := 0
	failCount := 0
	statusCodes := make(map[int]int)

	for res := range results {
		if res.Err != nil {
			failCount++
		} else {
			successCount++
			statusCodes[res.StatusCode]++
		}
	}

	totalDuration := time.Since(startTime)

	// --- OUTPUT STATISTIK ---
	fmt.Println("================ STATISTIK ================")
	fmt.Printf("Waktu Pengujian     : %v\n", totalDuration)
	fmt.Printf("Request Sukses      : %d\n", successCount)
	fmt.Printf("Request Gagal/Error : %d\n", failCount)
	fmt.Println("Detail Status Code  :")
	for code, count := range statusCodes {
		fmt.Printf("  - Status %d: %d kali\n", code, count)
	}
	fmt.Println("===========================================")
}

// Fungsi worker yang berjalan secara konkuren
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

		// Pastikan body ditutup untuk menghindari memory leak
		resp.Body.Close()

		results <- Result{StatusCode: resp.StatusCode, Duration: duration}
	}
}

