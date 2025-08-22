// 分析exp_6数据生成和查询逻辑
#include <iostream>
#include <vector>
#include <set>

int main() {
    // 模拟数据生成逻辑
    int orders_rows = 1000;
    int lineitem_rows = 3000;
    int64_t start_date = 19940101;
    int64_t end_date = 19940401;
    
    std::cout << "=== 数据生成分析 ===" << std::endl;
    
    // 1. Orders表分析
    std::cout << "Orders表:" << std::endl;
    std::vector<int64_t> priorities = {1, 2, 3, 4, 5};
    int priority_counts[6] = {0}; // 索引0不用，1-5对应优先级
    int date_filtered_count = 0;
    
    for (int i = 0; i < orders_rows; i++) {
        int64_t orderkey = i + 1;
        int64_t priority = priorities[i % 5];
        int64_t orderdate = 19940101 + (i % 120); // 日期范围
        
        priority_counts[priority]++;
        
        // 检查日期过滤条件
        if (orderdate >= start_date && orderdate < end_date) {
            date_filtered_count++;
        }
    }
    
    std::cout << "总订单数: " << orders_rows << std::endl;
    for (int p = 1; p <= 5; p++) {
        std::cout << "优先级 " << p << ": " << priority_counts[p] << " 个订单" << std::endl;
    }
    std::cout << "日期过滤后订单数: " << date_filtered_count << std::endl;
    
    // 2. Lineitem表分析
    std::cout << "\nLineitem表:" << std::endl;
    int commit_less_receipt_count = 0;
    std::set<int64_t> valid_orderkeys; // 满足commitdate < receiptdate的orderkey
    
    for (int i = 0; i < lineitem_rows; i++) {
        int64_t orderkey = (i % orders_rows) + 1;
        int64_t commitdate = 19940101 + (i % 100);
        int64_t receiptdate = commitdate + (i % 2 == 0 ? 5 : -2);
        
        if (commitdate < receiptdate) {
            commit_less_receipt_count++;
            valid_orderkeys.insert(orderkey);
        }
    }
    
    std::cout << "总lineitem数: " << lineitem_rows << std::endl;
    std::cout << "满足 commitdate < receiptdate 的lineitem数: " << commit_less_receipt_count << std::endl;
    std::cout << "涉及的不同orderkey数: " << valid_orderkeys.size() << std::endl;
    
    // 3. 分析每个orderkey的lineitem分布
    std::cout << "\n每个orderkey的lineitem分析:" << std::endl;
    std::cout << "每个orderkey有 " << lineitem_rows / orders_rows << " 个lineitem" << std::endl;
    
    // 检查前几个orderkey的情况
    for (int orderkey = 1; orderkey <= 5; orderkey++) {
        std::cout << "Orderkey " << orderkey << " 的lineitem:" << std::endl;
        bool has_valid = false;
        for (int i = 0; i < lineitem_rows; i++) {
            if ((i % orders_rows) + 1 == orderkey) {
                int64_t commitdate = 19940101 + (i % 100);
                int64_t receiptdate = commitdate + (i % 2 == 0 ? 5 : -2);
                std::cout << "  i=" << i << ", commitdate=" << commitdate 
                         << ", receiptdate=" << receiptdate 
                         << ", valid=" << (commitdate < receiptdate ? "YES" : "NO") << std::endl;
                if (commitdate < receiptdate) has_valid = true;
            }
        }
        std::cout << "  Orderkey " << orderkey << " 通过EXISTS检查: " << (has_valid ? "YES" : "NO") << std::endl;
    }
    
    // 4. 最终结果分析
    std::cout << "\n=== 最终结果分析 ===" << std::endl;
    
    // 每个orderkey都有3个lineitem (3000/1000=3)
    // lineitem的索引分布：orderkey=1对应i=0,1000,2000; orderkey=2对应i=1,1001,2001...
    
    std::cout << "每个orderkey的lineitem索引分析:" << std::endl;
    for (int orderkey = 1; orderkey <= 5; orderkey++) {
        std::cout << "Orderkey " << orderkey << " 对应的lineitem索引: ";
        bool will_pass_exists = false;
        for (int j = 0; j < 3; j++) { // 每个orderkey有3个lineitem
            int i = (orderkey - 1) + j * orders_rows;
            std::cout << i << " ";
            // 检查这个lineitem是否满足条件
            if (i % 2 == 0) { // 偶数索引，receiptdate = commitdate + 5
                will_pass_exists = true;
            }
        }
        std::cout << " -> 通过EXISTS: " << (will_pass_exists ? "YES" : "NO") << std::endl;
    }
    
    // 详细分析日期过滤
    std::cout << "\n=== 详细日期过滤分析 ===" << std::endl;
    std::cout << "日期范围: [" << start_date << ", " << end_date << ")" << std::endl;
    std::cout << "即: [19940101, 19940401)" << std::endl;
    std::cout << "订单日期生成: 19940101 + (i % 120)" << std::endl;
    std::cout << "所以订单日期范围: 19940101 到 19940220 (19940101+119)" << std::endl;
    std::cout << "所有订单日期都 < 19940401，所以都通过日期过滤！" << std::endl;
    
    // 重新分析EXISTS检查
    std::cout << "\n=== 重新分析EXISTS检查 ===" << std::endl;
    int pass_exists_count = 0;
    int pass_exists_by_priority[6] = {0};
    
    for (int orderkey = 1; orderkey <= 1000; orderkey++) {
        bool will_pass = false;
        // 检查这个orderkey的所有lineitem
        for (int j = 0; j < 3; j++) {
            int i = (orderkey - 1) + j * 1000;
            if (i % 2 == 0) { // 偶数索引满足条件
                will_pass = true;
                break;
            }
        }
        
        if (will_pass) {
            pass_exists_count++;
            int priority = priorities[(orderkey - 1) % 5];
            pass_exists_by_priority[priority]++;
        }
    }
    
    std::cout << "通过EXISTS检查的订单总数: " << pass_exists_count << std::endl;
    for (int p = 1; p <= 5; p++) {
        std::cout << "优先级 " << p << " 通过EXISTS的订单数: " << pass_exists_by_priority[p] << std::endl;
    }
    
    // 结论
    std::cout << "\n=== 修正后的结论 ===" << std::endl;
    std::cout << "1. 每个优先级有200个订单 (1000/5=200)" << std::endl;
    std::cout << "2. 日期过滤：所有1000个订单都通过日期过滤" << std::endl;
    std::cout << "3. EXISTS检查：只有偶数orderkey通过 (orderkey=1,3,5,7,9...)" << std::endl;
    std::cout << "4. 最终结果应该是每个优先级100个订单" << std::endl;
    std::cout << "5. 实际运行结果 '每个优先级100个' 是正确的！" << std::endl;
    
    return 0;
}
