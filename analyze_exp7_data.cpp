#include <iostream>
#include <vector>
#include <cstdint>

int main() {
    std::cout << "=== EXP_7 数据分析 ===" << std::endl;
    
    // 测试数据（与exp_7.cpp中相同）
    std::vector<int64_t> test_shipdates = {
        19940201, 19940301, 19940401, 19940501, 19940601,  // Records 0-4: in range [19940101, 19950101)
        19930101, 19950201, 19960101, 19970101, 19980101   // Records 5-9: out of range
    };
    
    std::vector<int64_t> test_discounts = {
        5, 6, 7, 5, 6,     // Records 0-4: in range [5, 7] (discount_center=6, ±1)
        3, 4, 8, 9, 10     // Records 5-9: out of range
    };
    
    std::vector<int64_t> test_quantities = {
        10, 15, 20, 23, 22,  // Records 0-4: < 24 (threshold)
        25, 30, 35, 40, 45   // Records 5-9: >= 24
    };
    
    std::vector<int64_t> test_prices = {
        100000, 200000, 300000, 400000, 500000,  // Records 0-4: 1000.00, 2000.00, 3000.00, 4000.00, 5000.00
        600000, 700000, 800000, 900000, 1000000  // Records 5-9: 6000.00, 7000.00, 8000.00, 9000.00, 10000.00
    };

    // 过滤条件
    int64_t start_date = 19940101;
    int64_t end_date = 19950101;
    int64_t discount_min = 5;
    int64_t discount_max = 7;
    int64_t quantity_threshold = 24;

    std::cout << "过滤条件:" << std::endl;
    std::cout << "  Shipdate: [" << start_date << ", " << end_date << ")" << std::endl;
    std::cout << "  Discount: [" << discount_min << ", " << discount_max << "]" << std::endl;
    std::cout << "  Quantity: < " << quantity_threshold << std::endl;
    std::cout << std::endl;

    std::cout << "逐步过滤分析:" << std::endl;
    
    // Step 1: Shipdate filtering
    std::vector<int> step1_passed;
    for (int i = 0; i < 10; i++) {
        bool pass = (test_shipdates[i] >= start_date && test_shipdates[i] < end_date);
        if (pass) {
            step1_passed.push_back(i);
        }
        std::cout << "Record " << i << ": shipdate=" << test_shipdates[i] 
                  << " -> " << (pass ? "PASS" : "FAIL") << std::endl;
    }
    std::cout << "Step 1 通过记录数: " << step1_passed.size() << std::endl;
    std::cout << std::endl;

    // Step 2: Discount filtering
    std::vector<int> step2_passed;
    for (int idx : step1_passed) {
        bool pass = (test_discounts[idx] >= discount_min && test_discounts[idx] <= discount_max);
        if (pass) {
            step2_passed.push_back(idx);
        }
        std::cout << "Record " << idx << ": discount=" << test_discounts[idx] 
                  << " -> " << (pass ? "PASS" : "FAIL") << std::endl;
    }
    std::cout << "Step 2 通过记录数: " << step2_passed.size() << std::endl;
    std::cout << std::endl;

    // Step 3: Quantity filtering
    std::vector<int> step3_passed;
    for (int idx : step2_passed) {
        bool pass = (test_quantities[idx] < quantity_threshold);
        if (pass) {
            step3_passed.push_back(idx);
        }
        std::cout << "Record " << idx << ": quantity=" << test_quantities[idx] 
                  << " -> " << (pass ? "PASS" : "FAIL") << std::endl;
    }
    std::cout << "Step 3 通过记录数: " << step3_passed.size() << std::endl;
    std::cout << std::endl;

    // Step 4: Revenue calculation
    int64_t total_revenue = 0;
    std::cout << "最终通过所有过滤的记录:" << std::endl;
    for (int idx : step3_passed) {
        int64_t revenue = test_prices[idx] * test_discounts[idx];
        total_revenue += revenue;
        std::cout << "Record " << idx << ": " << test_prices[idx] << " * " << test_discounts[idx] 
                  << " = " << revenue << std::endl;
    }
    
    std::cout << std::endl;
    std::cout << "=== 结果 ===" << std::endl;
    std::cout << "总收入 (raw): " << total_revenue << std::endl;
    std::cout << "总收入 (dollars): $" << (total_revenue / 100.0) << std::endl;
    
    return 0;
}
