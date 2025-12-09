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
    int d, m, y; // day, month, year
    char sep; // separator
    std::stringstream ss(s); // create stringstream from input string
    ss >> d >> sep >> m >> sep >> y; // match format DD/MM/YYYY
                                     // ss adalah stream yang membaca dari string s
    return (long long)y * 10000 + m * 100 + d; // return in format YYYYMMDD as long long
}

// ==========================================
// Bagian Header (Modified for CustomerData)
// ==========================================

class ParallelMergeSort { 
private:
    std::vector<CustomerData> *data; // Modified from vector<int> to vector<CustomerData>

    // Fungsi rekursif untuk melakukan merge sort
    // available_threads menunjukkan berapa banyak thread yang bisa digunakan
    void recursiveSort(int left, int right, int available_threads);

public:
    ParallelMergeSort(std::vector<CustomerData> *data); // Konstruktor
    ~ParallelMergeSort(); // Destructor
    
    // Fungsi utama yang dipanggil user
    void sort(); // Mulai proses sorting
};

// ==========================================
// Bagian Implementasi (Modified for CustomerData)
// ==========================================

ParallelMergeSort::ParallelMergeSort(std::vector<CustomerData> *data) // Konstruktor dengan
    : data(data) { // Inisialisasi pointer ke data
}

ParallelMergeSort::~ParallelMergeSort() {} // Destructor

void ParallelMergeSort::recursiveSort(int left, int right, int available_threads) {
    // Jika data kecil, urutkan langsung dengan std::sort (Sequential)
    // Threshold 5000 digunakan untuk menyeimbangkan overhead thread
    const int THRESHOLD = 5000; // Batas data untuk beralih ke sort sequential, 
                                // jika data lebih kecil dari ini maka langsung gunakan std::sort
    
    if (right - left < THRESHOLD) { // Base case: gunakan std::sort untuk data kecil
        // Modified: Use lambda to compare based on sort_key
        std::sort(data->begin() + left, data->begin() + right + 1,
            [](const CustomerData &a, const CustomerData &b) {
                return a.sort_key < b.sort_key;
            }); 
        return;
    }
    
    if (left >= right) { // Base case: subarray dengan 0 atau 1 elemen langsung dikembalikan
        return;
    }

    int mid = left + (right - left) / 2; // Cari titik tengah dari array
                                         // titik tengah digunakan untuk membagi array

    // Jika masih ada thread yang bisa dipakai (>1), pecah tugas ke thread baru
    if (available_threads > 1) {
        // Thread baru mengerjakan sisi kiri dengan setengah jumlah thread tersisa
        std::thread thread_left([this, left, mid, available_threads] { // this adalah pointer ke objek ParallelMergeSort, left dan mid adalah batas array
            this->recursiveSort(left, mid, available_threads / 2); // Gunakan setengah thread untuk sisi kiri
        });

        // Thread saat ini (Current Thread) mengerjakan sisi kanan dengan sisa thread setelah dipakai kiri
        this->recursiveSort(mid + 1, right, available_threads - (available_threads / 2)); // Sisa thread untuk sisi kanan

        // Tunggu thread kiri selesai
        thread_left.join(); // Menunggu thread kiri selesai sebelum melanjutkan

    } else {
        // Jika thread tersedia sudah habis, jalankan rekursif biasa (single thread)
        this->recursiveSort(left, mid, 1); // Hanya 1 thread untuk sisi kiri
        this->recursiveSort(mid + 1, right, 1); // Hanya 1 thread untuk sisi kanan
    }
    
    // Merge dua bagian yang sudah terurutkan
    std::vector<CustomerData> result; // buat vector sementara untuk menyimpan hasil merge
    result.reserve(right - left + 1); // Optimasi alokasi memori

    int i = left; // Pointer untuk bagian kiri
    int j = mid + 1; // Pointer untuk bagian kanan

    while (i <= mid && j <= right) { // Jika i kurang dari mid dan j kurang dari right
        // Modified: Compare sort_key
        if ((*data)[i].sort_key <= (*data)[j].sort_key) { // Jika elemen i lebih kecil atau sama dengan elemen j
            result.push_back((*data)[i]); // Tambahkan elemen i ke result 
            i++;
        } else {
            result.push_back((*data)[j]); // tambahkan elemen j ke result
            j++;
        }
    }

    while (i <= mid) { 
        result.push_back((*data)[i]); // Tambahkan sisa elemen i ke result
        i++;
    }

    while (j <= right) {
        result.push_back((*data)[j]); // Tambahkan sisa elemen j ke result
        j++;
    }

    // Salin kembali hasil merge ke array utama
    for (int k = 0; k < result.size(); k++) {
        (*data)[left + k] = result[k]; // Salin elemen dari result ke data
    }
}

void ParallelMergeSort::sort() {
    if (!data || data->empty()) { // Cek jika data kosong
        return;                   // Jika kosong, tidak perlu di-sort
    }

    // Deteksi otomatis jumlah Core CPU
    unsigned int cores = std::thread::hardware_concurrency();
    
    // Safety check: jika hardware_concurrency return 0 (gagal), set default ke 2
    if (cores == 0) cores = 2;

    // Panggil fungsi rekursif dengan memberikan core yang tersedia
    recursiveSort(0, data->size() - 1, cores);  // Mulai dari indeks 0 sampai panjang size-1
                                                // size adalah variabel yang berisi jumlah elemen dalam vector
}

// ==========================================
// Bagian Main & CSV Handling
// ==========================================

// Global variable untuk header CSV agar bisa disimpan ulang
std::string csv_header = "";

// Fungsi membaca CSV (Adapted from Code B)
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
                // Remove potential decimal points or handle float to long conversion carefully
                key = std::stoll(std::to_string((long long)(std::stod(price) * 100))); // harga -> cent
            }
            else if (sortField == "invoice_date") {
                key = convertDate(invoice_date); // YYYYMMDD
            }
        } catch (...) {
            continue; // Skip lines with parse errors
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

    // Inisialisasi ParallelMergeSort dengan pointer ke vector CustomerData
    ParallelMergeSort* sorter = new ParallelMergeSort(&data_customers);
    
    // Ukur waktu
    auto start = std::chrono::high_resolution_clock::now();
    
    sorter->sort(); // Panggil metode sort untuk memulai proses sorting
    
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
    out["duration"] = duration.count(); // Duration in milliseconds
    out["data"] = arr;

    // Output formatted JSON
    std::cout << "---START_JSON---\n";
    std::cout << out.dump(2) << "\n";
    std::cout << "---END_JSON---\n";

    // Debugging info (optional, output to cerr to not break JSON parsing)
    std::cerr << "ParallelMergeSort selesai dalam: " << duration.count() << " ms." << std::endl;
    std::cerr << "Core yang digunakan: " << std::thread::hardware_concurrency() << std::endl;

    // Bersihkan memori
    delete sorter;
    
    return 0;
}