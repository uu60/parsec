import random

def to_signed(bits, value):
    mask = (1 << bits) - 1
    if bits > 1 and value & (1 << (bits - 1)):
        return value | ~mask
    return value & mask

def rand(bits):
    if bits == 1:
        return random.randint(0, 1)
    return random.randint(- (1 << (bits - 1)), (1 << (bits - 1)) - 1)

def format_shares_for_cpp(bits):
    bool_shares_0 = []
    bool_shares_1 = []
    arith_shares_0 = []
    arith_shares_1 = []

    for _ in range(triplets_count):
        # Generate a random bit r
        r = rand(1)

        # Boolean sharing
        r_bool_share1 = rand(1)
        r_bool_share2 = r ^ r_bool_share1  # XOR for boolean share

        # Arithmetic sharing
        r_arith_share1 = rand(bits)
        r_arith_share2 = to_signed(bits, r - r_arith_share1)  # Addition for arithmetic share

        # Append formatted shares
        bool_shares_0.append(f"{r_bool_share1}")
        bool_shares_1.append(f"{r_bool_share2}")
        arith_shares_0.append(f"{r_arith_share1}")
        arith_shares_1.append(f"{r_arith_share2}")

    bool_shares_0 = ['true' if x == '1' else 'false' for x in bool_shares_0]
    bool_shares_1 = ['true' if x == '1' else 'false' for x in bool_shares_1]
    bool_shares_cpp = f"{{{', '.join(bool_shares_0)}}}\n{{{', '.join(bool_shares_1)}}}"
    arith_shares_cpp = f"{{{', '.join(arith_shares_0)}}}\n{{{', '.join(arith_shares_1)}}}"

    return bool_shares_cpp, arith_shares_cpp


if __name__ == '__main__':
    # Generate and format shares
    bit_sizes = [8, 16, 32, 64]
    triplets_count = 100

    for bits in bit_sizes:
        print("\n" + str(bits))
        bool_shares_cpp, arith_shares_cpp = format_shares_for_cpp(bits)
        # Print the C++ formatted arrays
        print(f"{bool_shares_cpp}")
        print(f"{arith_shares_cpp}")