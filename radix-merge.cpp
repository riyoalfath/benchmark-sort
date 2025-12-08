#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <algorithm>
#include <random>

// --- Global Variables (mirip static di Java) ---
const int length = 10-000-000;
std::vector<short> data3(length);
const int numThreads = 4;

// --- Implementasi CyclicBarrier untuk C++ ---
class CyclicBarrier {
private:
    std::mutex m;
    std::condition_variable cv;
    int threshold;
    int count;
    int generation;

public:
    explicit CyclicBarrier(int count) : threshold(count), count(count), generation(0) {}

    void await() {
        std::unique_lock<std::mutex> lock(m);
        int gen = generation;

        if (--count == 0) {
            generation++;
            count = threshold;
            cv.notify_all();
        } else {
            cv.wait(lock, [this, gen] { return gen != generation; });
        }
    }
};

// --- Helper Functions ---

// Mencari nilai maksimum di range tertentu
short getMax(int start, int end) {
    short maxVal = data3[start];
    for (int i = start + 1; i < end; i++) {
        if (data3[i] > maxVal) {
            maxVal = data3[i];
        }
    }
    return maxVal;
}

// Counting Sort berdasarkan digit (exp)
void countSort(int start, int end, int exp) {
    int n = end - start;
    std::vector<short> output(n);
    int count[10] = {0};

    // Hitung frekuensi
    for (int i = start; i < end; i++) {
        count[(data3[i] / exp) % 10]++;
    }

    // Ubah count[i] menjadi posisi
    for (int i = 1; i < 10; i++) {
        count[i] += count[i - 1];
    }

    // Build array output
    for (int i = end - 1; i >= start; i--) {
        int digit = (data3[i] / exp) % 10;
        output[count[digit] - 1] = data3[i];
        count[digit]--;
    }

    // Copy kembali ke data3
    for (int i = 0; i < n; i++) {
        data3[start + i] = output[i];
    }
}

// Fungsi utama Radix Sort
void radixSort(int start, int end) {
    short maxVal = getMax(start, end);

    for (int exp = 1; maxVal / exp > 0; exp *= 10) {
        countSort(start, end, exp);
    }
}

// Fungsi Merge
void mergeParts(int start, int end) {
    int mid = (start + end) / 2;
    std::vector<short> temp;
    temp.reserve(end - start);

    int i = start;
    int j = mid;

    while (i < mid && j < end) {
        if (data3[i] <= data3[j]) {
            temp.push_back(data3[i++]);
        } else {
            temp.push_back(data3[j++]);
        }
    }
    while (i < mid) temp.push_back(data3[i++]);
    while (j < end) temp.push_back(data3[j++]);

    for (int k = 0; k < temp.size(); k++) {
        data3[start + k] = temp[k];
    }
}

// --- Thread Worker Function ---
void threadWorker(int myPart, CyclicBarrier& barrier) {
    int rowsPerThread = length / 4;
    int start = myPart * rowsPerThread;
    int end = (myPart == 3) ? length : start + rowsPerThread;

    // 1. Sorting Lokal (Radix Sort)
    radixSort(start, end);

    // 
    // Ilustrasi bagaimana thread berjalan terpisah (Fork) lalu menunggu di Barrier (Join point)
    
    // Tunggu semua thread selesai sorting
    barrier.await();

    // 2. Merging Tahap 1
    if (myPart == 1) {
        // Gabung Part 0 dan 1
        mergeParts(0, end); 
    } else if (myPart == 3) {
        // Gabung Part 2 dan 3
        mergeParts(rowsPerThread * 2, length);
    }

    // Tunggu Merging Tahap 1 selesai
    barrier.await();

    // 3. Merging Tahap 2 (Final)
    if (myPart == 0) {
        mergeParts(0, length);
    }
}

// --- Main ---
int main() {
    // Random Number Generation
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 30000);

    for (int i = 0; i < length; i++) {
        data3[i] = (short) dis(gen);
    }

    std::cout << "Starting Parallel Radix-Merge Sort (C++)..." << std::endl;

    CyclicBarrier barrier(numThreads);
    std::vector<std::thread> threads;

    auto start_time = std::chrono::high_resolution_clock::now();

    // Buat Thread
    for (int i = 0; i < numThreads; i++) {
        threads.emplace_back(threadWorker, i, std::ref(barrier));
    }

    // Join Thread
    for (auto& t : threads) {
        t.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);

    std::cout << "Parallel Radix-Merge Sort Millis: " << duration_ms.count() << "ms" << std::endl;
    std::cout << "Parallel Radix-Merge Sort Nano: " << duration_ns.count() << "ns" << std::endl;

    // Verify
    bool correct = true;
    for (int i = 1; i < length; i++) {
        if (data3[i] < data3[i-1]) {
            correct = false;
            std::cout << "Error at index " << i << ": " << data3[i-1] << " > " << data3[i] << std::endl;
            break;
        }
    }
    if (correct) std::cout << "SUCCESS: Array is sorted." << std::endl;
    
    return 0;
}