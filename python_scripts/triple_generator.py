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

def format_triplets_for_cpp(bits):
    formatted_triplets_0 = []
    formatted_triplets_1 = []
    for i in range(100):
        a1, a2 = rand(bits), rand(bits)
        b1, b2 = rand(bits), rand(bits)
        r = rand(bits)
        c = to_signed(bits, to_signed(bits, a1 + a2) * to_signed(bits, b1 + b2))
        r1, r2 = to_signed(bits, r), to_signed(bits, c - r)
        if f"{{{a1}, {b1}, {r1}}}" not in formatted_triplets_0:
            formatted_triplets_0.append(f"{{{a1}, {b1}, {r1}}}")
            formatted_triplets_1.append(f"{{{a2}, {b2}, {r2}}}")

    print(len(formatted_triplets_0))
    return f"{{{{{', '.join(formatted_triplets_0)}}}}}" + f"\n{{{{{', '.join(formatted_triplets_1)}}}}}"


if __name__ == '__main__':
    # Generate and format triplets for different bit sizes
    bit_sizes = [1, 8, 16, 32, 64]
    triplets_count = 100  # Number of triplets to generate for each bit size

    for bits in bit_sizes:
        cpp_code = format_triplets_for_cpp(bits)
        print(cpp_code)
        print()