package gkafka

type Param map[string]any

func (p Param) Marshal() []byte {
	value, _ := JsonMarshal(p)
	return value
}

// Exists 是否存在
func (p Param) Exists(key string) bool {
	_, ok := p[key]
	return ok
}

// Get 获取字符串
func (p Param) Get(key string) string {
	value, _ := p[key].(string)
	return value
}

// GetStringSlice 获取string切片
func (p Param) GetStringSlice(key string) []string {
	itemList, _ := p[key].([]any)
	if itemList == nil {
		return nil
	}

	var stringList = make([]string, 0, len(itemList))
	if len(itemList) < 1 {
		return stringList
	}

	for index := 0; index < len(itemList); index++ {
		if val, ok := itemList[index].(string); ok {
			stringList = append(stringList, val)
		}
	}

	return stringList
}

func (p Param) GetBool(key string) bool {
	value, _ := p[key].(bool)
	return value
}

// GetInt 获取Int
func (p Param) GetInt(key string) int {
	return int(p.GetInt64(key))
}

// GetIntSlice 获取int切片
func (p Param) GetIntSlice(key string) []int {
	itemList, _ := p[key].([]any)
	if itemList == nil {
		return nil
	}

	var intList = make([]int, 0, len(itemList))
	if len(itemList) < 1 {
		return intList
	}

	for index := 0; index < len(itemList); index++ {
		if val, ok := itemList[index].(float64); ok {
			intList = append(intList, int(val))
		}
	}

	return intList
}

// GetInt64 获取Int64
func (p Param) GetInt64(key string) int64 {
	value, _ := p[key].(float64)
	return int64(value)
}

// GetInt64Slice 获取int64切片
func (p Param) GetInt64Slice(key string) []int64 {
	itemList, _ := p[key].([]any)
	if itemList == nil {
		return nil
	}

	var int64List = make([]int64, 0, len(itemList))
	if len(itemList) < 1 {
		return int64List
	}

	for index := 0; index < len(itemList); index++ {
		if val, ok := itemList[index].(float64); ok {
			int64List = append(int64List, int64(val))
		}
	}

	return int64List
}

// GetUint32Slice 获取uint32切片
func (p Param) GetUint32Slice(key string) []uint32 {
	itemList, _ := p[key].([]any)
	if itemList == nil {
		return nil
	}

	var uint32List = make([]uint32, 0, len(itemList))
	if len(itemList) < 1 {
		return uint32List
	}

	for index := 0; index < len(itemList); index++ {
		if val, ok := itemList[index].(float64); ok {
			uint32List = append(uint32List, uint32(val))
		}
	}

	return uint32List
}

// GetUint8 获取Uint8
func (p Param) GetUint8(key string) uint8 {
	return uint8(p.GetInt64(key))
}

// GetFloat64 获取Float64
func (p Param) GetFloat64(key string) float64 {
	value, _ := p[key].(float64)
	return value
}

func (p Param) Clone() Param {
	res := make(Param, len(p))

	for k, v := range p {
		res[k] = v
	}
	return res
}
