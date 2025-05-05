package config

func ParseMapStringConfig[T any](data *T, configmap map[string]string, funcs map[string]func(*T, string) error) error {

	var err error
	for k, v := range configmap {
		if f, ok := funcs[k]; ok {
			err = f(data, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
