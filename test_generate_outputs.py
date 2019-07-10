#!/usr/bin/env python3

from refiner import generateOutputs

def test(key, value):
    dirPath, leafPath, content = generateOutputs(key, value)
    print(f'Original: {key}:{value}     {leafPath}:{content}')

if __name__ == '__main__':

    test(b'@@mail.ru', b'abc123')
    test(b'abc123@gmail.com', b'abc123')
    test(b'abcd@gmail.com', b'abc123')
    test(b'abc@gmail.com', b'abc123')
    test(b'ab@gmail.com', b'abc123')
    test(b'a@gmail.com', b'abc123')
    test(b'a', b'abc123')
    test(b'ab', b'abc123')
    test(b'abc', b'abc123')
    test(b'abcd', b'abc123')
    test(b'a_c-', b'abc123')
    test(b'a_-_', b'abc123')
    test(b'a_-', b'abc123')
    test(b'a_', b'abc123')
    test(b'_', b'abc123')
