#include <iostream>
#include <vector>
#include <cmath>
#include <algorithm>

// Bit reverse function
int bit_reverse(int x, int num_bits) {
    int result = 0;
    for (int i = 0; i < num_bits; i++) {
        if (x & (1 << i)) {
            result |= 1 << (num_bits - 1 - i);
        }
    }
    return result;
}

void test_butterfly_logic() {
    int numBuckets = 32;
    int numLayers = 5; // log2(32) = 5
    
    // Test case: key = 17 (binary: 10001)
    // Hash value: (17 * 31 + 17) % 32 = 544 % 32 = 0
    // But for butterfly, we should use key 17 directly
    
    int key = 17;
    int hash_value = (key * 31 + 17) % numBuckets;
    
    std::cout << "Key: " << key << " (binary: ";
    for (int i = 4; i >= 0; i--) {
        std::cout << ((key >> i) & 1);
    }
    std::cout << ")" << std::endl;
    
    std::cout << "Hash value: " << hash_value << std::endl;
    
    // Expected final bucket for butterfly network
    int expected_bucket = bit_reverse(key, numLayers);
    std::cout << "Expected bucket (bit_reverse): " << expected_bucket << std::endl;
    
    // Simulate butterfly routing decisions
    std::cout << "\nButterfly routing decisions:" << std::endl;
    for (int layer = 0; layer < numLayers; layer++) {
        int bit_position = numLayers - layer - 1;
        int bit = (key >> bit_position) & 1;
        std::cout << "Layer " << layer << ": check bit at position " << bit_position 
                  << " = " << bit << std::endl;
    }
    
    // The problem: using hash_value instead of key for routing
    std::cout << "\nIf using hash_value for routing:" << std::endl;
    for (int layer = 0; layer < numLayers; layer++) {
        int bit_position = numLayers - layer - 1;
        int bit = (hash_value >> bit_position) & 1;
        std::cout << "Layer " << layer << ": check bit at position " << bit_position 
                  << " = " << bit << std::endl;
    }
}

int main() {
    test_butterfly_logic();
    
    // Test all keys from 0-4 (as in your test case)
    std::cout << "\n\nTest case analysis (keys 0-4):" << std::endl;
    for (int key = 0; key < 5; key++) {
        int hash_value = (key * 31 + 17) % 32;
        int expected_bucket = bit_reverse(key, 5);
        std::cout << "Key " << key << ": hash=" << hash_value 
                  << ", expected_bucket=" << expected_bucket << std::endl;
    }
    
    return 0;
}
