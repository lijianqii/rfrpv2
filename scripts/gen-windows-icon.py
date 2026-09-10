#!/usr/bin/env python3
"""生成 rfrp 的 Windows 图标（16/32/48 三尺寸，32-bit BGRA + AND mask）。

图案：深蓝圆角方块（垂直渐变）+ 白色圆环 + 中心圆点（隧道/端口意象）。
输出：crates/rfrp-bin/resources/rfrp.ico
"""
import struct

SIZES = (16, 32, 48)


def in_rounded_rect(x, y, size, radius):
    if x < 0 or y < 0 or x >= size or y >= size:
        return False
    # 四角圆角：距角小于 radius 的点需在圆内
    for (cx, cy) in ((0, 0), (size, 0), (0, size), (size, size)):
        dx, dy = abs(x - cx), abs(y - cy)
        if dx < radius and dy < radius:
            return (radius - dx) ** 2 + (radius - dy) ** 2 <= radius * radius
    return True


def pixel(size, x, y):
    r = max(2, size // 6)
    if not in_rounded_rect(x, y, size, r):
        return (0, 0, 0, 0)  # 透明
    # 垂直渐变
    t = y / max(1, size - 1)
    r_, g_, b_ = int(29 + (11 - 29) * t), int(78 + (42 - 78) * t), int(216 + (107 - 216) * t)
    a = 255
    c = size / 2.0
    d = ((x - c) ** 2 + (y - c) ** 2) ** 0.5 / size
    ring_center = 0.30
    ring_rad = 0.13
    dot_rad = 0.07
    if abs(d - ring_center) <= ring_rad or d <= dot_rad:
        r_, g_, b_, a = 255, 255, 255, 255
    return (r_, g_, b_, a)


def build_image(size):
    header = struct.pack(
        "<IiiHHIIiiII",
        40, size, size * 2, 1, 32, 0, size * size * 4 + 0, 0, 0, 0, 0,
    )
    # XOR：BGRA，自底向上
    xor = bytearray()
    for yy in range(size - 1, -1, -1):
        for xx in range(size):
            r_, g_, b_, a = pixel(size, xx, yy)
            xor += bytes((b_, g_, r_, a))
    # AND mask：1bpp，自底向上，每行对齐 4 字节
    row_bytes = ((size + 31) // 32) * 4
    and_mask = bytearray()
    for yy in range(size - 1, -1, -1):
        row = bytearray(row_bytes)
        for xx in range(size):
            _, _, _, a = pixel(size, xx, yy)
            if a < 128:
                row[xx // 8] |= 0x80 >> (xx % 8)
        and_mask += row
    return header + bytes(xor) + bytes(and_mask)


def main():
    images = [build_image(s) for s in SIZES]
    out = bytearray()
    out += struct.pack("<HHH", 0, 1, len(SIZES))
    offset = 6 + 16 * len(SIZES)
    for s, img in zip(SIZES, images):
        out += struct.pack(
            "<BBBBHHII",
            s if s < 256 else 0, s if s < 256 else 0, 0, 0, 1, 32, len(img), offset,
        )
        offset += len(img)
    for img in images:
        out += img
    path = "crates/rfrp-bin/resources/rfrp.ico"
    with open(path, "wb") as f:
        f.write(out)
    print(f"wrote {path} ({len(out)} bytes)")


if __name__ == "__main__":
    main()