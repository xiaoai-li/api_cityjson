# you can write to stdout for debugging purposes, e.g.
# print("this is a debug message")

def solution(A):
    # write your code in Python 3.6
    l = len(A)
    for i in range(l):
        for j in range(i, l):
            val = A[i]
            if val > A[j]:
                tmp = A[j]
                A[j] = A[i]
                A[i] = tmp

    for i in range(l):
        if A[i] >= 0 and A[i + 1] >= A[i] + 2:
            return A[i] + 1

    return 1


print(solution([1, 3, 6, 4, 1, 2]))
