*** Settings ***
Documentation       Inhuman Insurance, Inc. Artificial Intelligence System robot.
...                 Produces traffic data work items.

Library             RPA.RobotLogListener
Library             RPA.Tables
Library             Collections
Resource            shared.robot


*** Variables ***
${TRAFFIC_JSON_FILE_PATH}=      ${OUTPUT_DIR}${/}traffic.json
# JSON data keys:
${COUNTRY_KEY}=                 SpatialDim
${GENDER_KEY}=                  Dim1
${GENDER_VALUE}=                BTSX
${RATE_KEY}=                    NumericValue
${YEAR_KEY}=                    TimeDim
${RATE_VALUE_MAX}=              ${5.0}


*** Tasks ***
Produce traffic data work items
    Download traffic data
    ${traffic_data}=    Load traffic data as table
    ${traffic_data_filtered}=    Filter and sort traffic data    ${traffic_data}
    ${traffic_data_filtered}=    Get latest data by country    ${traffic_data_filtered}
    Log    ${traffic_data_filtered}
    ${payloads}=    Create work item payloads    ${traffic_data_filtered}
    Save work item payloads    ${payloads}


*** Keywords ***
Download traffic data
    Download
    ...    https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json
    ...    ${TRAFFIC_JSON_FILE_PATH}
    ...    overwrite=True

Load traffic data as table
    ${traffic_json}=    Load JSON from file    ${TRAFFIC_JSON_FILE_PATH}
    ${traffic_table}=    Create Table    ${traffic_json}[value]

    Log    ${traffic_table}
    Write table to CSV    ${traffic_table}    traffic.csv
    RETURN    ${traffic_table}

Filter and sort traffic data
    [Arguments]    ${table}
    Filter Table By Column    ${table}    ${RATE_KEY}    <    ${RATE_VALUE_MAX}
    Filter Table By Column    ${table}    ${GENDER_KEY}    ==    ${GENDER_VALUE}
    Sort table by column    ${table}    ${YEAR_KEY}    ascending=${False}
    RETURN    ${table}

Get latest data by country
    [Arguments]    ${table}
    ${table}=    Group Table By Column    ${table}    ${COUNTRY_KEY}
    ${latest_data_by_country}=    Create List
    FOR    ${group}    IN    @{table}
        ${first_row}=    Pop Table Row    ${group}
        Append To List    ${latest_data_by_country}    ${first_row}
        Log    ${group}
    END
    RETURN    ${latest_data_by_country}

Create work item payloads
    [Arguments]    ${traffic_data}
    ${payloads}=    Create List
    FOR    ${row}    IN    @{traffic_data}
        ${payload}=    Create Dictionary
        ...    country=${row}[${COUNTRY_KEY}]
        ...    year=${row}[${YEAR_KEY}]
        ...    rate=${row}[${RATE_KEY}]
        Append To List    ${payloads}    ${payload}
    END
    RETURN    ${payloads}

Save work item payloads
    [Arguments]    ${payloads}
    FOR    ${payload}    IN    @{payloads}
        Save work item payload    ${payload}
    END

Save work item payload
    [Arguments]    ${payload}
    ${variables}=    Create Dictionary    ${WORK_ITEM_NAME}=${payload}
    Create Output Work Item    variables=${variables}    save=${True}
