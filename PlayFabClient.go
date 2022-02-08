package playfab

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const url = "https://%s.playfabapi.com/%s/%s"
const retries = 3
const conflictStatus = "Conflict"

type Logger interface {
	Debug(format string, v ...interface{})
	Info(format string, v ...interface{})
	Warn(format string, v ...interface{})
	Error(format string, v ...interface{})
}

type PlayFabError struct {
	originError error
	Body        []byte
	Method      string
	RespCode    int
}

func (e *PlayFabError) Error() string {
	return fmt.Sprintf("%s - %s", e.Method, e.originError.Error())
}

type Option func(pf *PlayFab)

func WithLogger(logger Logger) Option {
	return func(pf *PlayFab) {
		pf.logger = logger
	}
}

type PlayFab struct {
	logger         Logger
	secret         string
	catalogVersion string
	titleId        string
	hc             *http.Client
}

func New(secret, titleId, catalogVersion string, opts ...Option) (*PlayFab, error) {
	switch "" {
	case secret:
		return nil, fmt.Errorf("secret is required")
	case catalogVersion:
		return nil, fmt.Errorf("catalog version is required")
	case titleId:
		return nil, fmt.Errorf("titleId is required")
	}
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxConnsPerHost:     100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     time.Minute * 1,
	}
	pf := &PlayFab{
		secret:         secret,
		catalogVersion: catalogVersion,
		titleId:        titleId,
		logger:         &noopLogger{},
		hc: &http.Client{
			Transport: transport,
			Timeout:   time.Second * 10,
		},
	}
	for _, opt := range opts {
		opt(pf)
	}
	return pf, nil
}

func (pf *PlayFab) EvaluateRandomTable(tableId string, playFabId string) (string, error) {
	requestBody, err := json.Marshal(map[string]string{
		"TableId":        tableId,
		"PlayFabId":      playFabId,
		"CatalogVersion": pf.catalogVersion,
	})

	if err != nil {
		return "", err
	}

	body, err := pf.request("POST", "Server", "EvaluateRandomResultTable", requestBody)

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

func (pf *PlayFab) UpdateUserReadOnlyData(data map[string]string, playFabId string) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"Data":      data,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return err
	}

	_, err = pf.request("POST", "Server", "UpdateUserReadOnlyData", requestBody)

	if err != nil {
		return err
	}

	return nil
}

func (pf *PlayFab) GetUserReadOnlyData(keys []string, playFabId string) (map[string]interface{}, error) {
	requestBody, err := json.Marshal(map[string]interface{}{
		"Keys":      keys,
		"PlayFabId": playFabId,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("POST", "Server", "GetUserReadOnlyData", requestBody)

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

func (pf *PlayFab) GrantItemsToUser(itemIds []string, playFabId string) ([]interface{}, error) {
	requestBody, err := json.Marshal(map[string]interface{}{
		"ItemIds":        itemIds,
		"PlayFabId":      playFabId,
		"CatalogVersion": pf.catalogVersion,
	})

	pf.logger.Debug("grant items to user playfabId: %s, itemIds %s", playFabId, itemIds)

	if err != nil {
		pf.logger.Debug("Failed Grant Items To User %v", err)
		return nil, err
	}

	body, err := pf.request("POST", "Server", "GrantItemsToUser", requestBody)

	pf.logger.Debug("grant items response %s", body)

	if err != nil {
		pf.logger.Debug("Failed Grant Items To User %v", err)
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

func (pf *PlayFab) GetPlayerStatistics(statisitcsIds []string, playFabId string) ([]map[string]interface{}, error) {
	pf.logger.Debug("starting ReadPlayerStatistics")
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":       playFabId,
		"StatisticsNames": statisitcsIds,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("GET", "Server", "GetPlayerStatistics", requestBody)

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

func (pf *PlayFab) GetPlayerCombinedInfo(reqInfo map[string]interface{}, playFabId string) (map[string]interface{}, error) {
	pf.logger.Debug("starting getplayercombinedinfo")
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":             playFabId,
		"InfoRequestParameters": reqInfo,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("POST", "Server", "GetPlayerCombinedInfo", requestBody)

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

func (pf *PlayFab) UpdatePlayerStatistics(statistics []interface{}, playFabId string) error {
	pf.logger.Debug("starting UpdatePlayerStatistics")
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":  playFabId,
		"Statistics": statistics,
	})

	if err != nil {
		return err
	}

	_, err = pf.request("POST", "Server", "UpdatePlayerStatistics", requestBody)

	if err != nil {
		return err
	}

	return nil
}

func (pf *PlayFab) GetTitleInternalData(keys []string) (map[string]interface{}, error) {
	pf.logger.Debug("starting GetTitleInternalData")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Keys": keys,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("POST", "Server", "GetTitleInternalData", requestBody)

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

func (pf *PlayFab) GetTitleData(keys []string) (map[string]interface{}, error) {
	pf.logger.Debug("starting GetTitleData")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Keys": keys,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("POST", "Server", "GetTitleData", requestBody)

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

func (pf *PlayFab) GetStoreItems(storeId string, playfabId string) ([]interface{}, string, error) {
	pf.logger.Debug("starting GetStoreItems")
	requestBody, err := json.Marshal(map[string]interface{}{
		"CatalogVersion": pf.catalogVersion,
		"StoreId":        storeId,
		"PlayFabId":      playfabId,
	})

	if err != nil {
		return nil, "", err
	}

	body, err := pf.request("POST", "Server", "GetStoreItems", requestBody)

	if err != nil {
		return nil, "", err
	}

	res := make(map[string]interface{})
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, "", err
	}

	data, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, "", fmt.Errorf("failed to parse GetStoreItem result")
	}

	storeItems, ok := data["Store"].([]interface{})
	if !ok {
		return nil, "", fmt.Errorf("failed to parse GetStoreItem result")
	}
	StoreId, ok := data["StoreId"].(string)
	if !ok {
		return nil, "", fmt.Errorf("failed to parse StoreId result")
	}
	pf.logger.Debug("Finished GetStoreItems")
	return storeItems, StoreId, nil
}

func (pf *PlayFab) GetStore(storeId string) (map[string]interface{}, error) {
	pf.logger.Debug("starting GetStore")
	requestBody, err := json.Marshal(map[string]interface{}{
		"CatalogVersion": pf.catalogVersion,
		"StoreId":        storeId,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("POST", "Server", "GetStoreItems", requestBody)

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

func (pf *PlayFab) GetCatalogItems() ([]interface{}, error) {
	pf.logger.Debug("starting GetCatalogItems")
	requestBody, err := json.Marshal(map[string]interface{}{
		"CatalogVersion": pf.catalogVersion,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("POST", "Server", "GetCatalogItems", requestBody)

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

func (pf *PlayFab) GetUserInventory(playFabId string) ([]interface{}, error) {

	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("POST", "Server", "GetUserInventory", requestBody)
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

func (pf *PlayFab) GetVirtualCurrency(playFabId string) (map[string]interface{}, error) {

	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("POST", "Server", "GetUserInventory", requestBody)
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

func (pf *PlayFab) AddUserVirtualCurrency(amount uint64, currencyId string, playFabId string) (map[string]interface{}, error) {
	pf.logger.Debug("starting AddUserVirtualCurrency")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Amount":          amount,
		"PlayFabId":       playFabId,
		"VirtualCurrency": currencyId,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("POST", "Server", "AddUserVirtualCurrency", requestBody)

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

func (pf *PlayFab) SubtractUserVirtualCurrency(amount uint64, currencyId string, playFabId string) (map[string]interface{}, error) {
	pf.logger.Debug("starting SubtractUserVirtualCurrency")
	requestBody, err := json.Marshal(map[string]interface{}{
		"Amount":          amount,
		"PlayFabId":       playFabId,
		"VirtualCurrency": currencyId,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("POST", "Server", "SubtractUserVirtualCurrency", requestBody)

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

func (pf *PlayFab) ConsumeItem(playFabId string, itemInstanceId string, consumeCount int) (interface{}, error) {
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId":      playFabId,
		"ItemInstanceId": itemInstanceId,
		"ConsumeCount":   consumeCount,
	})

	if err != nil {
		return nil, err
	}

	body, err := pf.request("POST", "Server", "ConsumeItem", requestBody)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func (pf *PlayFab) RevokeInventoryItems(revokeInventoryItems []map[string]interface{}) error {

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

	_, err = pf.request("POST", "Server", "RevokeInventoryItems", requestBody)

	if err != nil {
		return err
	}

	return nil
}

func (pf *PlayFab) SendPushNotification(message string, recipient string) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"Message":   message,
		"Recipient": recipient,
	})

	if err != nil {
		return err
	}

	_, err = pf.request("POST", "Server", "SendPushNotification", requestBody)

	if err != nil {
		return err
	}

	return nil
}

func (pf *PlayFab) AddPlayerTag(tag string, playFabId string) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
		"TagName":   tag,
	})

	if err != nil {
		return err
	}

	_, err = pf.request("POST", "Server", "AddPlayerTag", requestBody)

	if err != nil {
		return err
	}

	return nil
}

func (pf *PlayFab) RemovePlayerTag(tag string, playFabId string) error {
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
		"TagName":   tag,
	})

	if err != nil {
		return err
	}

	_, err = pf.request("POST", "Server", "RemovePlayerTag", requestBody)

	if err != nil {
		return err
	}

	return nil
}

func (pf *PlayFab) GetPlayerTags(playFabId string) ([]string, error) {
	requestBody, err := json.Marshal(map[string]interface{}{
		"PlayFabId": playFabId,
	})

	if err != nil {
		return nil, err
	}

	d, err := pf.request("POST", "Server", "GetPlayerTags", requestBody)

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

	for i := range t {
		v, ok := t[i].(string)

		if !ok {
			return nil, fmt.Errorf("Failed to parse tags result")
		}

		tags = append(tags, v)
	}

	return tags, nil
}

func (pf *PlayFab) request(method string, api string, funcName string, reqBody []byte) (d []byte, err error) {

	counter := 0

	for counter <= retries {
		counter++
		pf.logger.Debug("Starting retry %d for playfab request", counter)
		d, oerr := _request(pf.hc, method, pf.titleId, api, funcName, reqBody, pf.secret)
		if oerr != nil {
			errorData := make(map[string]interface{})
			errorData, err = ConvertToPlayFabErrorJson(oerr)
			if err != nil {
				isServiceUnavailableError := strings.Contains(err.Error(), "Service Unavailable")
				isBadRequestError := strings.Contains(err.Error(), "Bad Request")
				isBadGateWay := strings.Contains(err.Error(), "Bad Gateway")
				if !isServiceUnavailableError && !isBadRequestError && !isBadGateWay {
					return d, err
				}
				pf.logger.Error("waiting for retry after error - %s", err.Error())
			} else {
				err, isConflictError := isConflictError(errorData)
				if err != nil {
					return d, err
				}

				if !isConflictError {
					return d, oerr
				}
			}
			time.Sleep(1 * time.Second)
		} else {
			return d, nil
		}
	}

	return d, err
}

func _request(hc *http.Client, method string, titleId string, api string, funcName string, reqBody []byte, secretKey string) ([]byte, error) {
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
		return resBody, &PlayFabError{
			originError: fmt.Errorf("Failed To Process Request With status code %d: %s", resp.StatusCode, string(resBody)),
			Body:        resBody,
			Method:      funcName,
			RespCode:    resp.StatusCode,
		}
	}

	return resBody, nil
}

func isConflictError(errorData map[string]interface{}) (error, bool) {
	errStatus, ok := errorData["status"].(string)

	if !ok {
		err := fmt.Errorf("Failed to parse status from error")
		return err, false
	}

	if errStatus != conflictStatus {
		return nil, false
	}

	return nil, true
}

func ConvertToPlayFabErrorJson(oerr error) (map[string]interface{}, error) {
	errorData := make(map[string]interface{})
	serr, ok := oerr.(*PlayFabError)
	if !ok {
		err := fmt.Errorf("Failed to convert to playfab error")
		return nil, err
	}

	err := json.Unmarshal(serr.Body, &errorData)
	if err != nil {
		err := fmt.Errorf(err.Error() + " originalError: " + string(serr.Body))
		return nil, err
	}

	return errorData, nil
}
