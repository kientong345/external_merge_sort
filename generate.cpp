#include <iostream>
#include <fstream>
#include <vector>
#include <random>
#include <thread>
#include <mutex>

void generate_parameters(const std::string& fname, uint64_t total_elements, uint64_t chunk_size = 100000) {
    std::ofstream outfile(fname, std::ios::binary | std::ios::out);
    if (!outfile) {
        std::cerr << "Không thể mở file để ghi!" << std::endl;
        return;
    }

    std::random_device rd;
    std::mt19937_64 gen(rd());

    std::uniform_int_distribution<uint64_t> dis(0, std::numeric_limits<uint64_t>::max());

    std::vector<uint64_t> buffer;
    buffer.reserve(chunk_size);

    uint64_t elements_generated = 0;
    
    std::cout << "Đang khởi tạo dữ liệu..." << std::endl;

    while (elements_generated < total_elements) {
        uint64_t to_generate = std::min(chunk_size, total_elements - elements_generated);
        
        buffer.clear();
        for (uint64_t i = 0; i < to_generate; ++i) {
            buffer.push_back(dis(gen));
        }

        outfile.write(reinterpret_cast<const char*>(buffer.data()), to_generate * sizeof(uint64_t));
        
        elements_generated += to_generate;

        if (elements_generated % (total_elements / 10) == 0 || elements_generated == total_elements) {
            std::cout << "Tiến độ: " << (elements_generated * 100 / total_elements) << "%" << std::endl;
        }
    }

    outfile.close();
    std::cout << "Hoàn thành! File lưu tại: " << fname << std::endl;
}

int main() {
    const uint64_t TOTAL_ELEMENTS = 400000000ULL; // 400tr

    generate_parameters("data.bin", TOTAL_ELEMENTS);

    return 0;
}