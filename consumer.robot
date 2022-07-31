*** Settings ***
Documentation       Inhuman Insurance, Inc. Artificial Intelligence System robot.
...                 Consumes traffic data work items.

Resource            shared.robot
Library             RPA.RobotLogListener


*** Tasks ***
Consume traffic data work items
    For Each Input Work Item    Process traffic data


*** Keywords ***
Process traffic data
    ${payload}=    Get Work Item Payload
    ${traffic_data}=    Set Variable    ${payload}[${WORK_ITEM_NAME}]
    ${valid}=    Validate traffic data    ${traffic_data}
    IF    ${valid}
        Post traffic data to sales system    ${traffic_data}
    ELSE
        Handle invalid traffic data    ${traffic_data}
    END

Validate traffic data
    [Arguments]    ${traffic_data}
    ${country}=    Get value from JSON    ${traffic_data}    $.country
    ${valid}=    Evaluate    len("${country}")==3
    RETURN    ${valid}

Post traffic data to sales system
    [Arguments]    ${traffic_data}
    ${status}    ${return}=    Run Keyword And Ignore Error
    ...    POST
    ...    https://robocorp.com/inhuman-insurance-inc/sales-system-api
    ...    json=${traffic_data}
    Log    ${return}
    Handle traffic API response    ${status}    ${return}    ${traffic_data}

Handle invalid traffic data
    [Arguments]    ${traffic_data}
    ${message}=    Set Variable    Invalid traffic data: ${traffic_data}
    Log    ${message}    WARN
    Release Input Work Item
    ...    state=FAILED
    ...    exception_type=BUSINESS
    ...    code=INVALID_TRAFFIC_DATA
    ...    message=${message}

Handle traffic API response
    [Arguments]    ${status}    ${return}    ${traffic_data}
    IF    "${status}" == "PASS"
        Handle SUCCESS
    ELSE
        Handle EXCEPTION    ${return}    ${traffic_data}
    END

Handle SUCCESS
    Release Input Work Item    DONE

Handle EXCEPTION
    [Arguments]    ${return}    ${traffic_data}
    Log    Traffic data post failed: ${traffic_data} ${return}
    ...    Error
    Release Input Work Item
    ...    state=failed
    ...    exception_type=APPLICATION
    ...    code=Traffic_data_post_failed
    ...    message=${return}
