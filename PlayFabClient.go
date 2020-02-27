package playfab

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const ServerUrl = "https://titleId.playfabapi.com/Server/%s"

func EvaluateRandomTable(tableId string, playFabId string, secretKey string) (string, error) {
	requestBody, err := json.Marshal(map[string]string{
		"TableId": tableId,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return "", err
	}

	body, err := request("POST", "EvaluateRandomResultTable", requestBody, secretKey)

	if err != nil {
		return "", err
	}

	res := make(map[string]interface{})
	// Note below, json.Unmarshal can only take a pointer as second argument
	if err := json.Unmarshal(body, &res); err != nil {
		return "", err
	}

	data, ok := res["data"].(map[string]interface{});

	if !ok {
		return "nil", fmt.Errorf("Failed to parse EvaluateRandomResultTable result")
	}

	itemId, ok := data["ResultItemId"].(string)

	if !ok {
		return "nil", fmt.Errorf("Failed to parse EvaluateRandomResultTable result")
	}

	return itemId, nil
}

func UpdateUserReadOnlyData(data map[string]string, playFabId string, secretKey string) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"Data": data,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return  err
	}

	_, err = request("POST", "UpdateUserReadOnlyData", requestBody, secretKey)

	if err != nil {
		return err
	}

	return nil
}

func GrantItemsToUser(itemIds []string, playFabId string, secretKey string) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"ItemIds": itemIds,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", "GrantItemsToUser", requestBody, secretKey)

	if err != nil {
		return err
	}


	return nil
}

func request(method string, funcName string ,reqBody []byte, secretKey string) ([]byte, error){
	hc := &http.Client{}

	req, err := http.NewRequest(method, fmt.Sprintf(ServerUrl, funcName), bytes.NewBuffer(reqBody))

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

	if (resp.StatusCode != 200) {
		return nil, fmt.Errorf("Failed To Process Request With status code %s: %s", resp.StatusCode ,string(resBody))
	}

	return resBody, nil
}
