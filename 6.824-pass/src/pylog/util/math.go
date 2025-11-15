package util

func MinInt(vars ...int) int {
	min := vars[0]
	for _, i := range vars {
		if i < min {
			min = i
		}
	}
	return min
}

func MaxInt(vars ...int) int {
	max := vars[0]
	for _, i := range vars {
		if i > max {
			max = i
		}
	}
	return max
}

func MajorityInt(arr []int) int {
	times := make(map[int]int)
	for _, num := range arr {
		times[num] += 1
	}
	limit := len(arr) / 2
	for num, cnt := range times {
		if cnt > limit {
			return num
		}
	}
	return -1
}
