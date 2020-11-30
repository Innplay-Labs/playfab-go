package playfab

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const url = "https://%s.playfabapi.com/%s/%s"

type PlayFabError struct {
	originError error
	Body        []byte
}

func (e *PlayFabError) Error() string {
	return e.originError.Error()
}

func EvaluateRandomTable(tableId string, titleId string, playFabId string, secretKey string, catalogVersion string) (string, error) {
	requestBody, err := json.Marshal(map[string]string{
		"TableId":        tableId,
		"PlayFabId":      playFabId,
		"CatalogVersion": catalogVersion,
	})

	if err != nil {
		return "", err
	}

	body, err := request("POST", titleId, "Server", "EvaluateRandomResultTable", requestBody, secretKey)

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

func UnlockContainerItem(data map[string]string, titleId string, playFabId string, secretKey string) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"ContainerItemId": data,
		"PlayFabId":       playFabId,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", titleId, "Server", "UnlockContainerItem", requestBody, secretKey)

	if err != nil {
		return err
	}

	return nil
}

func UpdateUserReadOnlyData(data map[string]string, titleId string, playFabId string, secretKey string) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"Data":      data,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", titleId, "Server", "UpdateUserReadOnlyData", requestBody, secretKey)

	if err != nil {
		return err
	}

	return nil
}

func GetUserReadOnlyData(keys []string, titleId string, playFabId string, secretKey string) (map[string]interface{}, error) {
	requestBody, err := json.Marshal(map[string]interface{}{
		"Keys":      keys,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetUserReadOnlyData", requestBody, secretKey)

	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})

	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetUserReadOnlyData data")
	}

	keysData, ok := data["Data"].(map[string]interface{})

	if !ok {
		return nil, fmt.Errorf("Failed to parse GetUserReadOnlyData data")
	}

	return keysData, nil
}

func GrantItemsToUser(itemIds []string, titleId string, playFabId string, secretKey string, catalogVersion string) ([]interface{}, error) {
	requestBody, err := json.Marshal(map[string]interface{}{
		"ItemIds":        itemIds,
		"PlayFabId":      playFabId,
		"CatalogVersion": catalogVersion,
	})

	fmt.Printf("grant items to user playfabId: %s, itemIds %s", playFabId, itemIds)

	if err != nil {
		fmt.Printf("Failed Grant Items To User %v", err)
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GrantItemsToUser", requestBody, secretKey)

	fmt.Printf("grant items response %s", body)

	if err != nil {
		fmt.Printf("Failed Grant Items To User %v", err)
		return nil, err
	}

	res := make(map[string]interface{})

	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GrantItemsToUser data")
	}

	itemsRes, ok := data["ItemGrantResults"].([]interface{})

	if !ok {
		return nil, fmt.Errorf("Failed to parse GrantItemsToUser ItemGrantResults")
	}

	return itemsRes, nil
}

func GetPlayerStatistics(statisitcsIds []string, titleId string, playFabId string, secretKey string) ([]map[string]interface{}, error) {
	fmt.Println("starting ReadPlayerStatistics")
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":       playFabId,
		"StatisticsNames": statisitcsIds,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("GET", titleId, "Server", "GetPlayerStatistics", requestBody, secretKey)

	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})

	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse  GetPLayerStatistics")
	}

	statisitcs, ok := data["Statistics"].([]map[string]interface{})

	if !ok {
		return nil, fmt.Errorf("Failed to parse  GetPLayerStatistics")
	}

	return statisitcs, nil
}

func GetPlayerCombinedInfo(reqInfo map[string]interface{}, titleId string, playFabId string, secretKey string) (map[string]interface{}, error) {
	fmt.Println("starting getplayercombinedinfo")
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":             playFabId,
		"InfoRequestParameters": reqInfo,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetPlayerCombinedInfo", requestBody, secretKey)

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

func UpdatePlayerStatistics(statistics []interface{}, titleId string, playFabId string, secretKey string) error {
	fmt.Println("starting UpdatePlayerStatistics")
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":  playFabId,
		"Statistics": statistics,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", titleId, "Server", "UpdatePlayerStatistics", requestBody, secretKey)

	if err != nil {
		return err
	}

	return nil
}

func GetTitleInternalData(keys []string, titleId string, secretKey string) (map[string]interface{}, error) {
	fmt.Println("starting GetTitleInternalData")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Keys": keys,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetTitleInternalData", requestBody, secretKey)

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

func GetTitleData(keys []string, titleId string, secretKey string) (map[string]interface{}, error) {
	fmt.Println("starting GetTitleData")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Keys": keys,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetTitleData", requestBody, secretKey)

	if err != nil {
		return nil, err
	}
	body = bytes.TrimPrefix(body, []byte("\xef\xbb\xbf"))
	res := make(map[string]interface{})
	// Note below, json.Unmarshal can only take a pointer as second argument
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetTitleData result")
	}

	titlelData, ok := data["Data"].(map[string]interface{})

	return titlelData, nil
}

func GetStoreItems(storeId string, titleId string, catalogVersion string, secretKey string) ([]interface{}, error) {
	fmt.Println("starting GetStoreItems")
	requestBody, err := json.Marshal(map[string]interface{}{
		"CatalogVersion": catalogVersion,
		"StoreId":        storeId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetStoreItems", requestBody, secretKey)

	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetStoreItem result")
	}

	storeItems, ok := data["Store"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetStoreItem result")
	}

	return storeItems, nil
}

func GetStore(storeId string, titleId string, catalogVersion string, secretKey string) (map[string]interface{}, error) {
	fmt.Println("starting GetStore")
	requestBody, err := json.Marshal(map[string]interface{}{
		"CatalogVersion": catalogVersion,
		"StoreId":        storeId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetStoreItems", requestBody, secretKey)

	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetStore result")
	}

	MarketingData, ok := data["MarketingData"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse MarketingData ")
	}

	metadata, ok := MarketingData["Metadata"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse Metadata")
	}

	return metadata, nil
}

func GetCatalogItems(catalogVersion string, titleId string, secretKey string) ([]interface{}, error) {
	fmt.Println("starting GetCatalogItems")
	requestBody, err := json.Marshal(map[string]interface{}{
		"CatalogVersion": catalogVersion,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetCatalogItems", requestBody, secretKey)

	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetCatalogItems result")
	}

	catalogItems, ok := data["Catalog"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetCatalogItems result")
	}

	return catalogItems, nil
}

func GetUserInventory(playFabId string, titleId string, secretKey string) ([]interface{}, error) {

	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetUserInventory", requestBody, secretKey)
	if err != nil {
		return nil, err
	}
	res := make(map[string]interface{})
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetUserInventory result")
	}

	itemInstances, ok := data["Inventory"].([]interface{})

	if !ok {
		return nil, fmt.Errorf("Failed to parse GetUserInventory result")
	}

	return itemInstances, nil
}

func GetVirtualCurrency(playFabId string, titleId string, secretKey string) (map[string]interface{}, error) {

	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetUserInventory", requestBody, secretKey)
	if err != nil {
		return nil, err
	}
	res := make(map[string]interface{})
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetUserCurrency result")
	}

	virtualCurrency, ok := data["VirtualCurrency"].(map[string]interface{})

	if !ok {
		return nil, fmt.Errorf("Failed to parse GetUserCurrency result")
	}

	return virtualCurrency, nil
}

func AddUserVirtualCurrency(amount uint64, titleId string, currencyId string, playFabId string, secretKey string) (map[string]interface{}, error) {
	fmt.Println("starting AddUserVirtualCurrency")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Amount":          amount,
		"PlayFabId":       playFabId,
		"VirtualCurrency": currencyId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "AddUserVirtualCurrency", requestBody, secretKey)

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

func SubtractUserVirtualCurrency(amount uint64, titleId string, currencyId string, playFabId string, secretKey string) (map[string]interface{}, error) {
	fmt.Println("starting SubtractUserVirtualCurrency")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Amount":          amount,
		"PlayFabId":       playFabId,
		"VirtualCurrency": currencyId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "SubtractUserVirtualCurrency", requestBody, secretKey)

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

func ConsumeItem(playFabId string, titleId string, secretKey string, itemInstanceId string, consumeCount int) (interface{}, error) {
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":      playFabId,
		"ItemInstanceId": itemInstanceId,
		"ConsumeCount":   consumeCount,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "ConsumeItem", requestBody, secretKey)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func request(method string, titleId string, api string, funcName string, reqBody []byte, secretKey string) ([]byte, error) {
	hc := &http.Client{}

	req, err := http.NewRequest(method, fmt.Sprintf(url, titleId, api, funcName), bytes.NewBuffer(reqBody))

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
		return resBody, &PlayFabError{fmt.Errorf("Failed To Process Request With status code %d: %s", resp.StatusCode, string(resBody)), resBody}
	}

	return resBody, nil
}
