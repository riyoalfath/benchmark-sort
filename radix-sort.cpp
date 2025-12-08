#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// --- Data Structure ---
struct CustomerData {
    std::string original_line;
    long long sort_key;
};

// --- Global Variables ---
int length = 0;
const int numThreads = 4;
// std::thread::hardware_concurrency()

std::string csv_header = "";

std::vector<CustomerData> data_customers;
std::vector<CustomerData> buffer;

int global_counts[numThreads][10];
int global_starts[10][numThreads];

// --- CyclicBarrier ---
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
            cv.wait(lock, [this, gen]{ return gen != generation; });
        }
    }
};

// Convert invoice date "DD/MM/YYYY" → YYYYMMDD
long long convertDate(const std::string &s) {
    int d, m, y;
    char sep;
    std::stringstream ss(s);
    ss >> d >> sep >> m >> sep >> y;
    return (long long)y * 10000 + m * 100 + d;
}

// 2. Baca CSV dengan sortField
void readCSV(const std::string &filename, const std::string &sortField)
{
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "[ERROR] Cannot open file\n";
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
            key = std::stoll(std::to_string((long long)(std::stod(price) * 100))); // harga → cent
        }
        else if (sortField == "invoice_date") {
            key = convertDate(invoice_date); // YYYYMMDD
        }

        data_customers.push_back({line, key});
    }

    length = data_customers.size();
    buffer.resize(length);
    file.close();
}

// Get max key
long long getMax() {
    long long mx = data_customers[0].sort_key;
    for (auto &x : data_customers)
        if (x.sort_key > mx) mx = x.sort_key;
    return mx;
}

// --- Worker Thread ---
void threadWorker(int myID, CyclicBarrier &barrier, long long maxVal)
{
    int rowsPerThread = length / numThreads;
    int start = myID * rowsPerThread;
    int end = (myID == numThreads - 1) ? length : start + rowsPerThread;

    for (long long exp = 1; maxVal / exp > 0; exp *= 10) {

        for (int i = 0; i < 10; i++)
            global_counts[myID][i] = 0;

        for (int i = start; i < end; i++) {
            int digit = (data_customers[i].sort_key / exp) % 10;
            global_counts[myID][digit]++;
        }
        barrier.await();

        if (myID == 0) {
            int total = 0;
            for (int d = 0; d < 10; d++) {
                for (int t = 0; t < numThreads; t++) {
                    global_starts[d][t] = total;
                    total += global_counts[t][d];
                }
            }
        }
        barrier.await();

        int my_indices[10];
        for (int d = 0; d < 10; d++)
            my_indices[d] = global_starts[d][myID];

        for (int i = start; i < end; i++) {
            int digit = (data_customers[i].sort_key / exp) % 10;
            buffer[my_indices[digit]++] = data_customers[i];
        }
        barrier.await();

        for (int i = start; i < end; i++)
            data_customers[i] = buffer[i];

        barrier.await();
    }
}

// --- MAIN ---
int main(int argc, char* argv[])
{
    if (argc < 2) {
        std::cerr << "ERROR: missing sort field\n";
        return 1;
    }

    std::string sortField = argv[1];

    readCSV("data/customer_shopping_data.csv", sortField);

    long long mx = getMax();
    CyclicBarrier barrier(numThreads);
    std::vector<std::thread> threads;

    auto t1 = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numThreads; i++)
        threads.emplace_back(threadWorker, i, std::ref(barrier), mx);

    for (auto &t : threads)
        t.join();

    auto t2 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);

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

    std::cout << "---START_JSON---\n";
    std::cout << out.dump(2) << "\n";
    std::cout << "---END_JSON---\n";
}
