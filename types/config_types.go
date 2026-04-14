package libatapp_types

type ConfigExistedIndex struct {
	ExistedSet  map[string]struct{}
	MapKeyIndex map[string]int
}

func (i *ConfigExistedIndex) MutableExistedSet() map[string]struct{} {
	if i.ExistedSet == nil {
		i.ExistedSet = make(map[string]struct{})
	}

	return i.ExistedSet
}

func (i *ConfigExistedIndex) MutableMapKeyIndex() map[string]int {
	if i.MapKeyIndex == nil {
		i.MapKeyIndex = make(map[string]int)
	}

	return i.MapKeyIndex
}

func CreateConfigExistedIndex() *ConfigExistedIndex {
	return &ConfigExistedIndex{
		ExistedSet:  make(map[string]struct{}),
		MapKeyIndex: make(map[string]int),
	}
}
