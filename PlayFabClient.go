package playfab

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const Url = "https://%s.playfabapi.com/%s/%s"
const Retries = 3
const ConflictStatus = "Conflict"

type Logger interface {
	Debug(format string, v ...interface{})
	Info(format string, v ...interface{})
	Warn(format string, v ...interface{})
	Error(format string, v ...interface{})
}

type PlayFabError struct {
	originError error
	Body        []byte
}

func (e *PlayFabError) Error() string {
	return e.originError.Error()
}

func EvaluateRandomTable(tableId string, titleId string, playFabId string, secretKey string, catalogVersion string, logger Logger) (string, error) {
	requestBody, err := json.Marshal(map[string]string{
		"TableId":        tableId,
		"PlayFabId":      playFabId,
		"CatalogVersion": catalogVersion,
	})

	if err != nil {
		return "", err
	}

	body, err := request("POST", titleId, "Server", "EvaluateRandomResultTable", requestBody, secretKey, logger)

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

func UpdateUserReadOnlyData(data map[string]string, titleId string, playFabId string, secretKey string, logger Logger) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"Data":      data,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", titleId, "Server", "UpdateUserReadOnlyData", requestBody, secretKey, logger)

	if err != nil {
		return err
	}

	return nil
}

func GetUserReadOnlyData(keys []string, titleId string, playFabId string, secretKey string, logger Logger) (map[string]interface{}, error) {
	requestBody, err := json.Marshal(map[string]interface{}{
		"Keys":      keys,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetUserReadOnlyData", requestBody, secretKey, logger)

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

func GrantItemsToUser(itemIds []string, titleId string, playFabId string, secretKey string, catalogVersion string, logger Logger) ([]interface{}, error) {
	requestBody, err := json.Marshal(map[string]interface{}{
		"ItemIds":        itemIds,
		"PlayFabId":      playFabId,
		"CatalogVersion": catalogVersion,
	})

	logger.Debug("grant items to user playfabId: %s, itemIds %s", playFabId, itemIds)

	if err != nil {
		logger.Debug("Failed Grant Items To User %v", err)
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GrantItemsToUser", requestBody, secretKey, logger)

	logger.Debug("grant items response %s", body)

	if err != nil {
		logger.Debug("Failed Grant Items To User %v", err)
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

func GetPlayerStatistics(statisitcsIds []string, titleId string, playFabId string, secretKey string, logger Logger) ([]map[string]interface{}, error) {
	logger.Debug("starting ReadPlayerStatistics")
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":       playFabId,
		"StatisticsNames": statisitcsIds,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("GET", titleId, "Server", "GetPlayerStatistics", requestBody, secretKey, logger)

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

func GetPlayerCombinedInfo(reqInfo map[string]interface{}, titleId string, playFabId string, secretKey string, logger Logger) (map[string]interface{}, error) {
	logger.Debug("starting getplayercombinedinfo")
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":             playFabId,
		"InfoRequestParameters": reqInfo,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetPlayerCombinedInfo", requestBody, secretKey, logger)

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

func UpdatePlayerStatistics(statistics []interface{}, titleId string, playFabId string, secretKey string, logger Logger) error {
	logger.Debug("starting UpdatePlayerStatistics")
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":  playFabId,
		"Statistics": statistics,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", titleId, "Server", "UpdatePlayerStatistics", requestBody, secretKey, logger)

	if err != nil {
		return err
	}

	return nil
}

func GetTitleInternalData(keys []string, titleId string, secretKey string, logger Logger) (map[string]interface{}, error) {
	logger.Debug("starting GetTitleInternalData")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Keys": keys,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetTitleInternalData", requestBody, secretKey, logger)

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

func GetTitleData(keys []string, titleId string, secretKey string, logger Logger) (map[string]interface{}, error) {
	logger.Debug("starting GetTitleData")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Keys": keys,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetTitleData", requestBody, secretKey, logger)

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

func GetStoreItems(storeId string, titleId string, catalogVersion string, secretKey string, logger Logger) ([]interface{}, error) {
	logger.Debug("starting GetStoreItems")
	requestBody, err := json.Marshal(map[string]interface{}{
		"CatalogVersion": catalogVersion,
		"StoreId":        storeId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetStoreItems", requestBody, secretKey, logger)

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
	logger.Debug("Finished GetStoreItems")
	return storeItems, nil
}

func GetStore(storeId string, titleId string, catalogVersion string, secretKey string, logger Logger) (map[string]interface{}, error) {
	logger.Debug("starting GetStore")
	requestBody, err := json.Marshal(map[string]interface{}{
		"CatalogVersion": catalogVersion,
		"StoreId":        storeId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetStoreItems", requestBody, secretKey, logger)

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

func GetCatalogItems(catalogVersion string, titleId string, secretKey string, logger Logger) ([]interface{}, error) {
	logger.Debug("starting GetCatalogItems")
	requestBody, err := json.Marshal(map[string]interface{}{
		"CatalogVersion": catalogVersion,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetCatalogItems", requestBody, secretKey, logger)

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

func GetUserInventory(playFabId string, titleId string, secretKey string, logger Logger) ([]interface{}, error) {

	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetUserInventory", requestBody, secretKey, logger)
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

func GetVirtualCurrency(playFabId string, titleId string, secretKey string, logger Logger) (map[string]interface{}, error) {

	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "GetUserInventory", requestBody, secretKey, logger)
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

func AddUserVirtualCurrency(amount uint64, titleId string, currencyId string, playFabId string, secretKey string, logger Logger) (map[string]interface{}, error) {
	logger.Debug("starting AddUserVirtualCurrency")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Amount":          amount,
		"PlayFabId":       playFabId,
		"VirtualCurrency": currencyId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "AddUserVirtualCurrency", requestBody, secretKey, logger)

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

func SubtractUserVirtualCurrency(amount uint64, titleId string, currencyId string, playFabId string, secretKey string, logger Logger) (map[string]interface{}, error) {
	logger.Debug("starting SubtractUserVirtualCurrency")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Amount":          amount,
		"PlayFabId":       playFabId,
		"VirtualCurrency": currencyId,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "SubtractUserVirtualCurrency", requestBody, secretKey, logger)

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

func ConsumeItem(playFabId string, titleId string, secretKey string, itemInstanceId string, consumeCount int, logger Logger) (interface{}, error) {
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":      playFabId,
		"ItemInstanceId": itemInstanceId,
		"ConsumeCount":   consumeCount,
	})

	if err != nil {
		return nil, err
	}

	body, err := request("POST", titleId, "Server", "ConsumeItem", requestBody, secretKey, logger)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func request(method string, titleId string, api string, funcName string, reqBody []byte, secretKey string, logger Logger) (d []byte, err error) {

	counter := 0

	for counter <= Retries {
		counter++
		logger.Debug("Starting retry %d for playfab request", counter)
		d, oerr := _request(method, titleId, api, funcName, reqBody, secretKey)
		if oerr != nil {
			err, isConflictError := isConflictError(oerr)
			if err != nil {
				return d, err
			}

			if !isConflictError {
				return d, oerr
			}

			time.Sleep(1 * time.Second)

		} else {
			return d, nil
		}
	}

	return d, err
}

func RevokeInventoryItems(revokeInventoryItems []map[string]interface{}, titleId string, secretKey string, logger Logger) error {

	// Make sure there are no empty/nil cells in the slice
	newRevokeInventoryItems := make([]interface{}, 0, len(revokeInventoryItems))
	for _, item := range revokeInventoryItems {
		if item != nil {
			newRevokeInventoryItems = append(newRevokeInventoryItems, item)
		}
	}

	// Nothing to delete - do nothing
	if len(newRevokeInventoryItems) == 0 {
		return nil
	}

	requestBody, err := json.Marshal(map[string]interface{}{
		"Items": newRevokeInventoryItems,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", titleId, "Server", "RevokeInventoryItems", requestBody, secretKey, logger)

	if err != nil {
		return err
	}

	return nil
}

func SendPushNotification(message string, recipient string, titleId string, secretKey string, logger Logger) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"Message":   message,
		"Recipient": recipient,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", titleId, "Server", "SendPushNotification", requestBody, secretKey, logger)

	if err != nil {
		return err
	}

	return nil
}

func AddPlayerTag(tag string, titleId string, playFabId string, secretKey string, logger Logger) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
		"TagName":   tag,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", titleId, "Server", "AddPlayerTag", requestBody, secretKey, logger)

	if err != nil {
		return err
	}

	return nil
}

func RemovePlayerTag(tag string, titleId string, playFabId string, secretKey string, logger Logger) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
		"TagName":   tag,
	})

	if err != nil {
		return err
	}

	_, err = request("POST", titleId, "Server", "RemovePlayerTag", requestBody, secretKey, logger)

	if err != nil {
		return err
	}

	return nil
}

func GetPlayerTags(titleId string, playFabId string, secretKey string, logger Logger) ([]string, error) {
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
	})

	if err != nil {
		return nil, err
	}

	d, err := request("POST", titleId, "Server", "GetPlayerTags", requestBody, secretKey, logger)

	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})
	if err := json.Unmarshal(d, &res); err != nil {
		return nil, err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Failed to parse GetPlayerCombinedInfo result")
	}

	t, ok := data["Tags"].([]interface{})

	tags := make([]string, 0)

	for i, _ := range t {
		v, ok := t[i].(string)

		if !ok {
			return nil, fmt.Errorf("Failed to parse tags result")
		}

		tags = append(tags, v)
	}

	return tags, nil
}

func _request(method string, titleId string, api string, funcName string, reqBody []byte, secretKey string) ([]byte, error) {
	hc := &http.Client{}

	req, err := http.NewRequest(method, fmt.Sprintf(Url, titleId, api, funcName), bytes.NewBuffer(reqBody))

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

func isConflictError(oerr error) (error, bool) {
	errorData := make(map[string]interface{})
	serr, ok := oerr.(*PlayFabError)
	if !ok {
		err := fmt.Errorf("Failed to convert to playfab error")
		return err, false
	}

	err := json.Unmarshal(serr.Body, &errorData)
	if err != nil {
		err := fmt.Errorf(err.Error() + " originalError: " + string(serr.Body))
		return err, false
	}

	errStatus, ok := errorData["status"].(string)

	if !ok {
		err := fmt.Errorf("Failed to parse status from error")
		return err, false
	}

	if errStatus != ConflictStatus {
		return nil, false
	}

	return nil, true
}
