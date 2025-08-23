package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

// Configuration for the test client
const (
	baseURL = "http://localhost:8080/kv"
	testKey = "go-test-key-456"
)

// A simple struct to decode the server's GET response
type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// A helper function to print formatted headers
func printHeader(title string) {
	fmt.Println("\n=================================================")
	fmt.Printf(" %s\n", title)
	fmt.Println("=================================================")
}

// A helper function to check for errors and exit if one occurs
func checkErr(err error, message string) {
	if err != nil {
		fmt.Printf("FATAL ERROR: %s - %v\n", message, err)
		os.Exit(1)
	}
}

func main() {
	client := &http.Client{}

	// --- 1. Test PUT (Create) ---
	printHeader("1. Testing PUT (Create)")
	putValue1 := "hello-from-go"
	fmt.Printf("Sending value '%s' to key '%s'...\n", putValue1, testKey)
	putBody1, _ := json.Marshal(map[string]string{"value": putValue1})
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s", baseURL, testKey), bytes.NewBuffer(putBody1))
	checkErr(err, "Creating PUT request")
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	checkErr(err, "Executing PUT request")
	defer resp.Body.Close()

	fmt.Printf("-> Server responded with status: %s\n", resp.Status)
	if resp.StatusCode != http.StatusCreated {
		fmt.Println("   FAIL: Expected status 201 Created")
	} else {
		fmt.Println("   PASS: Received expected status 201 Created")
	}

	// --- 2. Test GET (Read) ---
	printHeader("2. Testing GET (Read)")
	fmt.Printf("Retrieving key '%s'...\n", testKey)
	resp, err = http.Get(fmt.Sprintf("%s/%s", baseURL, testKey))
	checkErr(err, "Executing GET request")
	defer resp.Body.Close()

	fmt.Printf("-> Server responded with status: %s\n", resp.Status)
	if resp.StatusCode == http.StatusOK {
		var getResp GetResponse
		err = json.NewDecoder(resp.Body).Decode(&getResp)
		checkErr(err, "Decoding GET response")
		fmt.Printf("   Received value: '%s'\n", getResp.Value)
		if getResp.Value == putValue1 {
			fmt.Println("   PASS: Value matches what was sent.")
		} else {
			fmt.Printf("   FAIL: Expected '%s' but got '%s'\n", putValue1, getResp.Value)
		}
	} else {
		fmt.Printf("   FAIL: Expected status 200 OK, but got %s\n", resp.Status)
	}

	// --- 3. Test PUT (Update) ---
	printHeader("3. Testing PUT (Update)")
	putValue2 := "updated-value-from-go"
	fmt.Printf("Sending updated value '%s' to key '%s'...\n", putValue2, testKey)
	putBody2, _ := json.Marshal(map[string]string{"value": putValue2})
	req, err = http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s", baseURL, testKey), bytes.NewBuffer(putBody2))
	checkErr(err, "Creating update PUT request")
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	checkErr(err, "Executing update PUT request")
	defer resp.Body.Close()

	fmt.Printf("-> Server responded with status: %s\n", resp.Status)
	if resp.StatusCode != http.StatusCreated {
		fmt.Println("   FAIL: Expected status 201 Created")
	} else {
		fmt.Println("   PASS: Received expected status 201 Created")
	}

	// --- 4. Test GET (Verify Update) ---
	printHeader("4. Testing GET (Verify Update)")
	fmt.Printf("Retrieving key '%s' again...\n", testKey)
	resp, err = http.Get(fmt.Sprintf("%s/%s", baseURL, testKey))
	checkErr(err, "Executing GET request after update")
	defer resp.Body.Close()

	fmt.Printf("-> Server responded with status: %s\n", resp.Status)
	if resp.StatusCode == http.StatusOK {
		var getResp GetResponse
		err = json.NewDecoder(resp.Body).Decode(&getResp)
		checkErr(err, "Decoding GET response after update")
		fmt.Printf("   Received value: '%s'\n", getResp.Value)
		if getResp.Value == putValue2 {
			fmt.Println("   PASS: Value matches the updated value.")
		} else {
			fmt.Printf("   FAIL: Expected '%s' but got '%s'\n", putValue2, getResp.Value)
		}
	} else {
		fmt.Printf("   FAIL: Expected status 200 OK, but got %s\n", resp.Status)
	}

	// --- 5. Test DELETE ---
	printHeader("5. Testing DELETE")
	fmt.Printf("Deleting key '%s'...\n", testKey)
	req, err = http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/%s", baseURL, testKey), nil)
	checkErr(err, "Creating DELETE request")

	resp, err = client.Do(req)
	checkErr(err, "Executing DELETE request")
	defer resp.Body.Close()

	fmt.Printf("-> Server responded with status: %s\n", resp.Status)
	if resp.StatusCode == http.StatusOK {
		fmt.Println("   PASS: Received expected status 200 OK")
	} else {
		fmt.Printf("   FAIL: Expected status 200 OK, but got %s\n", resp.Status)
	}

	// --- 6. Test GET (Verify Delete) ---
	printHeader("6. Testing GET (Verify Delete)")
	fmt.Printf("Retrieving deleted key '%s'...\n", testKey)
	resp, err = http.Get(fmt.Sprintf("%s/%s", baseURL, testKey))
	checkErr(err, "Executing GET request after delete")
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	fmt.Printf("-> Server responded with status: %s\n", resp.Status)
	fmt.Printf("   Response body: %s\n", string(bodyBytes))
	if resp.StatusCode == http.StatusNotFound {
		fmt.Println("   PASS: Received expected status 404 Not Found")
	} else {
		fmt.Printf("   FAIL: Expected status 404 Not Found, but got %s\n", resp.Status)
	}

	fmt.Println("\n--- Test complete ---")
}
