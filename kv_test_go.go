package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// Define the URLs for our regional servers
const (
	serverUSEast = "http://localhost:8080"
	serverUSWest = "http://localhost:8081"
	serverEUWest = "http://localhost:8082"
	testKey      = "comprehensive-geo-test-key"
)

// A simple struct to decode the server's GET response
type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// --- Helper Functions ---

func printHeader(title string) {
	fmt.Println("\n=================================================")
	fmt.Printf(" %s\n", title)
	fmt.Println("=================================================")
}

func checkErr(err error, message string) {
	if err != nil {
		fmt.Printf("FATAL ERROR: %s - %v\n", message, err)
		os.Exit(1)
	}
}

// A generic client to perform a PUT request
func putValue(serverURL, key, value string) {
	fmt.Printf("-> PUT to %s with value '%s'\n", serverURL, value)
	client := &http.Client{}
	putBody, _ := json.Marshal(map[string]string{"value": value})
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/kv/%s", serverURL, key), bytes.NewBuffer(putBody))
	checkErr(err, "Creating PUT request")
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	checkErr(err, "Executing PUT request")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		fmt.Printf("   FAIL: Expected status 201 Created, but got %s\n", resp.Status)
	} else {
		fmt.Printf("   PASS: Received expected status %s\n", resp.Status)
	}
}

// A generic client to perform a GET request and verify the value
func getValue(serverURL, key, expectedValue string, expectFound bool) {
	fmt.Printf("-> GET from %s, expecting value '%s' (found=%t)\n", serverURL, expectedValue, expectFound)
	resp, err := http.Get(fmt.Sprintf("%s/kv/%s", serverURL, key))
	checkErr(err, "Executing GET request")
	defer resp.Body.Close()

	if !expectFound {
		if resp.StatusCode == http.StatusNotFound {
			fmt.Printf("   PASS: Received expected status %s\n", resp.Status)
		} else {
			fmt.Printf("   FAIL: Expected status 404 Not Found, but got %s\n", resp.Status)
		}
		return
	}

	if resp.StatusCode == http.StatusOK {
		var getResp GetResponse
		err = json.NewDecoder(resp.Body).Decode(&getResp)
		checkErr(err, "Decoding GET response")
		if getResp.Value == expectedValue {
			fmt.Printf("   PASS: Received expected value '%s'\n", getResp.Value)
		} else {
			fmt.Printf("   FAIL: Expected '%s' but got '%s'\n", expectedValue, getResp.Value)
		}
	} else {
		fmt.Printf("   FAIL: Expected status 200 OK, but got %s\n", resp.Status)
	}
}

// A generic client to perform a DELETE request
func deleteValue(serverURL, key string) {
	fmt.Printf("-> DELETE from %s for key '%s'\n", serverURL, key)
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/kv/%s", serverURL, key), nil)
	checkErr(err, "Creating DELETE request")

	resp, err := client.Do(req)
	checkErr(err, "Executing DELETE request")
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("   PASS: Received expected status %s\n", resp.Status)
	} else {
		fmt.Printf("   FAIL: Expected status 200 OK, but got %s\n", resp.Status)
	}
}

// --- Main Test Execution ---
func main() {
	printHeader("Starting Comprehensive Geo-Distributed Test")

	// 1. Write to US-East
	initialValue := "data-from-east"
	putValue(serverUSEast, testKey, initialValue)

	// Wait for replication to occur
	fmt.Println("\n... Waiting 3 seconds for replication ...")
	time.Sleep(3 * time.Second)

	// 3. Read from all regions
	printHeader("Test 2: Read Data from all")
	getValue(serverUSEast, testKey, initialValue, true)
	getValue(serverUSWest, testKey, initialValue, true)
	getValue(serverEUWest, testKey, initialValue, true)

	// 4. Update from US-West
	printHeader("Test 3: Update Value from a Different Region")
	updatedValue := "updated-in-the-west"
	putValue(serverUSWest, testKey, updatedValue)

	fmt.Println("\n... Waiting 3 seconds for replication ...")
	time.Sleep(3 * time.Second)

	// 5. Read the update from all regions
	printHeader("Test 4: Verify Replicated Update")
	getValue(serverUSEast, testKey, updatedValue, true)
	getValue(serverUSWest, testKey, updatedValue, true)
	getValue(serverEUWest, testKey, updatedValue, true)

	// 6. Cleanup: Delete from any region
	printHeader("Test 5: Delete Key")
	deleteValue(serverEUWest, testKey)

	fmt.Println("\n... Waiting 3 seconds for replication ...")
	time.Sleep(3 * time.Second)

	// 7. Verify deletion across all regions
	printHeader("Test 6: Verify Deletion Across All Regions")
	getValue(serverUSEast, testKey, "", false)
	getValue(serverUSWest, testKey, "", false)
	getValue(serverEUWest, testKey, "", false)

	printHeader("Comprehensive Test Complete")

}
