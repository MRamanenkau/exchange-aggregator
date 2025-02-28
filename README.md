# Rust Test Task: Crypto Exchange Aggregator

An exchange aggregator designed to collect cryptocurrency market data.

## Table of Contents

- [Features Implemented](#features-implemented)
- [Planned Improvements](#planned-improvements)
- [Installation](#installation)

## Features Implemented

1. ✅ **Historical Klines Retrieval**
    - Dynamic endpoint generation for data access.
    - Efficient fetching of historical Kline data from exchanges.
2. ✅ **Database Storage for Klines** 
    - Seamless saving of retrieved Kline data into a persistent database.
3. ❌ **Recent Trades Retrieval**
4. ❌ **Database Storage for Recent Trades** 

## Planned Improvements

1. **Lazy Endpoint Management**
    - Optimize endpoint generation and handling for improved performance and resource efficiency.
2. **Comprehensive Logging**
    - Implement detailed logging to enhance debugging and monitoring capabilities.
3. **Robust Error Handling**
    - Strengthen error management to ensure reliability across diverse scenarios.
4. **Edge Case Handling**
    - Address uncommon but critical use cases for a more resilient system.
5. **Progress Indicators**
    - Add a progress bar to provide visual feedback during data retrieval and processing.

## Installation

Follow these steps to set up the project locally:

1. **Initialize the Database**
   ```bash
   cd ./src/db && bash ./init.sh

1. **Build the Project**
   ```bash
   cargo build