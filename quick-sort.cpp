#include <iostream>
#include <vector>
#include <chrono> 
#include <cstdlib> 
#include <thread> 
#include <algorithm> 
#include <fstream>
#include <sstream>
#include <string>
#include <mutex>
#include <utility> // For std::swap
#include <nlohmann/json.hpp> // Requires nlohmann/json library

using json = nlohmann::json;

// ==========================================
// Data Structures & Helpers
// ==========================================

struct CustomerData {
    std::string original_line;
    long long sort_key;
};

// Convert invoice date "DD/MM/YYYY" -> YYYYMMDD
long long convertDate(const std::string &s) {
    int d, m, y;
    char sep;
    std::stringstream ss(s);
    ss >> d >> sep >> m >> sep >> y;
    return (long long)y * 10000 + m * 100 + d;
}

// ==========================================
// Bagian Header (ParallelQuickSort)
// ==========================================

class ParallelQuickSort { 
private:
    std::vector<CustomerData> *data; 

    // Helper untuk mempartisi array (Lomuto Partition Scheme)
    int partition(int low, int high);

    // Fungsi rekursif untuk melakukan quick sort
    // available_threads menunjukkan berapa banyak thread yang bisa digunakan
    void recursiveSort(int left, int right, int available_threads);

public:
    ParallelQuickSort(std::vector<CustomerData> *data); // Konstruktor
    ~ParallelQuickSort(); // Destructor
    
    // Fungsi utama yang dipanggil user
    void sort(); // Mulai proses sorting
};

// ==========================================
// Bagian Implementasi (ParallelQuickSort)
// ==========================================

ParallelQuickSort::ParallelQuickSort(std::vector<CustomerData> *data) // Konstruktor
    : data(data) { 
}

ParallelQuickSort::~ParallelQuickSort() {} // Destructor

// Logika Partitioning (Memilih Pivot dan memindahkan elemen)
int ParallelQuickSort::partition(int low, int high) {
    long long pivot = (*data)[high].sort_key; // Ambil elemen terakhir sebagai pivot
    int i = (low - 1); // Index elemen yang lebih kecil

    for (int j = low; j <= high - 1; j++) {
        // Jika elemen saat ini lebih kecil dari pivot
        if ((*data)[j].sort_key < pivot) {
            i++;
            std::swap((*data)[i], (*data)[j]);
        }
    }
    std::swap((*data)[i + 1], (*data)[high]);
    return (i + 1); // Kembalikan posisi pivot
}

void ParallelQuickSort::recursiveSort(int left, int right, int available_threads) {
    // Jika data kecil, urutkan langsung dengan std::sort (Sequential)
    // Threshold 5000 digunakan untuk menyeimbangkan overhead thread
    const int THRESHOLD = 100000; 
    
    // Base case: jika range tidak valid atau data sedikit
    if (left >= right) return;

    if (right - left < THRESHOLD) { 
        std::sort(data->begin() + left, data->begin() + right + 1, 
            [](const CustomerData &a, const CustomerData &b) {
                return a.sort_key < b.sort_key;
            }); 
        return;
    }

    // Lakukan partisi: elemen < pivot ke kiri, elemen > pivot ke kanan
    int pi = partition(left, right);

    // --- LOGIKA UTAMA PARALLEL ---

    // Jika masih ada thread yang bisa dipakai (>1), pecah tugas ke thread baru
    if (available_threads > 1) {
        // Thread baru mengerjakan sisi kiri pivot (left ... pi-1)
        // Kita beri dia setengah dari jatah thread
        std::thread thread_left([this, left, pi, available_threads] {
            this->recursiveSort(left, pi - 1, available_threads / 2); 
        });

        // Thread saat ini (Current Thread) mengerjakan sisi kanan pivot (pi+1 ... right)
        // Dia mengambil sisa thread
        this->recursiveSort(pi + 1, right, available_threads - (available_threads / 2));

        // Tunggu thread kiri selesai
        thread_left.join();

    } else {
        // Jika thread tersedia sudah habis, jalankan rekursif biasa (single thread)
        this->recursiveSort(left, pi - 1, 1);
        this->recursiveSort(pi + 1, right, 1);
    }
}

void ParallelQuickSort::sort() {
    if (!data || data->empty()) {
        return;
    }

    // Deteksi otomatis jumlah Core CPU
    unsigned int cores = std::thread::hardware_concurrency(); // std::thread::hardware_concurrency()
    
    // Safety check: jika hardware_concurrency return 0 (gagal), set default ke 2
    if (cores == 0) cores = 2;

    // Panggil fungsi rekursif dengan memberikan core yang tersedia
    recursiveSort(0, data->size() - 1, cores);  
}

// ==========================================
// Bagian Main & CSV Handling
// ==========================================

std::string csv_header = "";

// Fungsi membaca CSV
void readCSV(const std::string &filename, const std::string &sortField, std::vector<CustomerData> &data_customers)
{
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "[ERROR] Cannot open file: " << filename << "\n";
        exit(1);
    }

    std::getline(file, csv_header);
    std::string line;

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::stringstream ss(line);
        std::string invoice_no, customer_id, gender, age, category;
        std::string quantity, price, payment_method, invoice_date, shopping_mall;

        std::getline(ss, invoice_no, ',');
        std::getline(ss, customer_id, ',');
        std::getline(ss, gender, ',');
        std::getline(ss, age, ',');
        std::getline(ss, category, ',');
        std::getline(ss, quantity, ',');
        std::getline(ss, price, ',');
        std::getline(ss, payment_method, ',');
        std::getline(ss, invoice_date, ',');
        std::getline(ss, shopping_mall, ',');

        long long key = 0;

        try {
            if (sortField == "invoice_no") {
                key = std::stoll(invoice_no.substr(1));
            }
            else if (sortField == "customer_id") {
                key = std::stoll(customer_id.substr(1));
            }
            else if (sortField == "quantity") {
                key = std::stoll(quantity);
            }
            else if (sortField == "price") {
                key = std::stoll(std::to_string((long long)(std::stod(price) * 100))); 
            }
            else if (sortField == "invoice_date") {
                key = convertDate(invoice_date); 
            }
        } catch (...) {
            continue; 
        }

        data_customers.push_back({line, key});
    }

    file.close();
}

int main(int argc, char* argv[]) {
    
    // Check command line arguments
    if (argc < 2) {
        std::cerr << "ERROR: missing sort field (e.g., ./program invoice_no)\n";
        return 1;
    }

    std::string sortField = argv[1];
    
    // Vector untuk menyimpan data
    std::vector<CustomerData> data_customers;

    // Membaca data CSV
    readCSV("data/customer_shopping_data.csv", sortField, data_customers);

    // Inisialisasi ParallelQuickSort
    ParallelQuickSort* sorter = new ParallelQuickSort(&data_customers);
    
    // Ukur waktu
    auto start = std::chrono::high_resolution_clock::now();
    
    sorter->sort(); // Panggil metode sort
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Persiapkan Output JSON
    json arr = json::array();

    for (const auto &item : data_customers) {
        std::stringstream ss(item.original_line);
        std::string col;
        json row;

        std::getline(ss, col, ','); row["invoice_no"] = col;
        std::getline(ss, col, ','); row["customer_id"] = col;
        std::getline(ss, col, ','); row["gender"] = col;
        std::getline(ss, col, ','); row["age"] = col;
        std::getline(ss, col, ','); row["category"] = col;
        std::getline(ss, col, ','); row["quantity"] = col;
        std::getline(ss, col, ','); row["price"] = col;
        std::getline(ss, col, ','); row["payment_method"] = col;
        std::getline(ss, col, ','); row["invoice_date"] = col;
        std::getline(ss, col, ','); row["shopping_mall"] = col;

        arr.push_back(row);
    }

    json out;
    out["duration"] = duration.count();
    out["data"] = arr;

    // Output formatted JSON
    std::cout << "---START_JSON---\n";
    std::cout << out.dump(2) << "\n";
    std::cout << "---END_JSON---\n";

    // Debugging info
    std::cerr << "ParallelQuickSort selesai dalam: " << duration.count() << " ms." << std::endl;
    std::cerr << "Core yang digunakan: " << std::thread::hardware_concurrency() << std::endl;

    // Bersihkan memori
    delete sorter;
    
    return 0;
}