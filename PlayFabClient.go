package playfab

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const url = "https://titleId.playfabapi.com/%s/%s"

func EvaluateRandomTable(tableId string, playFabId string, secretKey string) (string, error) {
	requestBody, err := json.Marshal(map[string]string{
		"TableId":   tableId,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return "", err
	}

	body, err := request("POST", "Server", "EvaluateRandomResultTable", requestBody, secretKey)

	if err != nil {
		return "", err
	}

	res := make(map[string]interface{})
	// Note below, json.Unmarshal can only take a pointer as second argument
	if err := json.Unmarshal(body, &res); err != nil {
		return "", err
	}

	data, ok := res["data"].(map[string]interface{})

	if !ok {
		return "", fmt.Errorf("Failed to parse EvaluateRandomResultTable result")
	}

	itemId, ok := data["ResultItemId"].(string)

	if !ok {
		return "", fmt.Errorf("Failed to parse EvaluateRandomResultTable result")

	}

	return itemId, nil
}

func UpdateUserReadOnlyData(data map[string]string, playFabId string, secretKey string) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"Data":      data,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", "Server", "UpdateUserReadOnlyData", requestBody, secretKey)

	if err != nil {
		return err
	}

	return nil
}

func GrantItemsToUser(itemIds []string, playFabId string, secretKey string) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"ItemIds":   itemIds,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", "Server", "GrantItemsToUser", requestBody, secretKey)

	if err != nil {
		return err
	}

	return nil
}

func GetPlayerCombinedInfo(reqInfo map[string]interface{}, playFabId string, secretKey string) (map[string]interface{}, error) {
	fmt.Println("starting getplayercombinedinfo")
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":             playFabId,
		"InfoRequestParameters": reqInfo,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", "Server", "GetPlayerCombinedInfo", requestBody, secretKey)

	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})
	// Note below, json.Unmarshal can only take a pointer as second argument
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetPlayerCombinedInfo result")
	}

	infoRes, ok := data["InfoResultPayload"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetPlayerCombinedInfo result")
	}

	return infoRes, nil
}

func UpdatePlayerStatistics(statistics []interface{}, playFabId string, secretKey string) error {
	fmt.Println("starting UpdatePlayerStatistics")
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":  playFabId,
		"Statistics": statistics,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", "Server", "UpdatePlayerStatistics", requestBody, secretKey)

	if err != nil {
		return err
	}

	return nil
}

func GetTitleInternalData(keys []string, secretKey string) (map[string]interface{}, error) {
	fmt.Println("starting GetTitleInternalData")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Keys": keys,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", "Server", "GetTitleInternalData", requestBody, secretKey)

	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})
	// Note below, json.Unmarshal can only take a pointer as second argument
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetTitleInternalData result")
	}

	internalData, ok := data["Data"].(map[string]interface{})

	return internalData, nil
}

func AddUserVirtualCurrency(amount uint64, currencyId string, playFabId string, secretKey string) (map[string]interface{}, error) {
	fmt.Println("starting AddUserVirtualCurrency")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Amount":          amount,
		"PlayFabId":       playFabId,
		"VirtualCurrency": currencyId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", "Server", "AddUserVirtualCurrency", requestBody, secretKey)

	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse AddUserVirtualCurrencyResponse result")
	}

	return data, nil
}

func SubtractUserVirtualCurrency(amount uint64, currencyId string, playFabId string, secretKey string) (map[string]interface{}, error) {
	fmt.Println("starting SubtractUserVirtualCurrency")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Amount":          amount,
		"PlayFabId":       playFabId,
		"VirtualCurrency": currencyId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", "Server", "SubtractUserVirtualCurrency", requestBody, secretKey)

	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse SubtractUserVirtualCurrency result")
	}

	return data, nil
}

func request(method string, api string, funcName string, reqBody []byte, secretKey string) ([]byte, error) {
	hc := &http.Client{}

	req, err := http.NewRequest(method, fmt.Sprintf(url, api, funcName), bytes.NewBuffer(reqBody))

	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-type", "application/json")
	req.Header.Add("X-SecretKey", secretKey)

	resp, err := hc.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	resBody, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Failed To Process Request With status code %s: %s", resp.StatusCode, string(resBody))
	}

	return resBody, nil
}
