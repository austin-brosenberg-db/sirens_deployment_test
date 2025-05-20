from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, BinaryType

# Define DataType schema
DataType = StructType([
    StructField("_VALUE", StringType(), True),
    StructField("_Name", StringType(), True),
    # StructField("_Type", StringType(), True)
])

# Define ComplexDataType schema
ComplexDataType = StructType([
    StructField("Data", ArrayType(DataType), True),
    StructField("_Name", StringType(), True)
])

# Define SystemPropertiesType schema
SystemPropertiesType = StructType([
    StructField("Provider", StructType([
        StructField("_Name", StringType(), True),
        StructField("_Guid", StringType(), True),
        StructField("_EventSourceName", StringType(), True)
    ]), True),
    StructField("EventID", StructType([
        StructField("_VALUE", StringType(), True),
        StructField("_Qualifiers", StringType(), True)
    ]), True),
    StructField("Version", StringType(), True),
    StructField("Level", StringType(), True),
    StructField("Task", StringType(), True),
    StructField("Opcode", StringType(), True),
    StructField("Keywords", StringType(), True),
    StructField("TimeCreated", StructType([
        StructField("_SystemTime", StringType(), True),
        StructField("_RawTime", StringType(), True)
    ]), True),
    StructField("EventRecordID", StringType(), True),
    StructField("Correlation", StructType([
        StructField("_ActivityID", StringType(), True),
        StructField("_RelatedActivityID", StringType(), True)
    ]), True),
    StructField("Execution", StructType([
        StructField("_ProcessID", StringType(), True),
        StructField("_ThreadID", StringType(), True),
        StructField("_ProcessorID", StringType(), True),
        StructField("_SessionID", StringType(), True),
        StructField("_KernelTime", StringType(), True),
        StructField("_UserTime", StringType(), True),
        StructField("_ProcessorTime", StringType(), True)
    ]), True),
    StructField("Channel", StringType(), True),
    StructField("Computer", StringType(), True),
    StructField("Security", StructType([
        StructField("_UserID", StringType(), True)
    ]), True),
    StructField("any", MapType(StringType(), StringType()), True)  # For <xs:any>
])

# Define ProcessingErrorDataType schema
ProcessingErrorDataType = StructType([
    StructField("ErrorCode", StringType(), True),
    StructField("DataItemName", StringType(), True),
    StructField("EventPayload", StringType(), True),
    StructField("any", MapType(StringType(), StringType()), True)  # For <xs:any>
])

# Define RenderingInfoType schema
RenderingInfoType = StructType([
    StructField("Message", StringType(), True),
    StructField("Level", StringType(), True),
    StructField("Opcode", StringType(), True),
    StructField("Task", StringType(), True),
    StructField("Channel", StringType(), True),
    StructField("Provider", StringType(), True),
    StructField("Keywords", StructType([
        StructField("Keyword", StringType(), True)
    ]), True),
    StructField("any", MapType(StringType(), StringType()), True),  # For <xs:any>
    StructField("Culture", StringType(), True)
])

# Define DebugDataType schema
DebugDataType = StructType([
    StructField("SequenceNumber", StringType(), True),
    StructField("FlagsName", StringType(), True),
    StructField("LevelName", StringType(), True),
    StructField("Component", StringType(), True),
    StructField("SubComponent", StringType(), True),
    StructField("FileLine", StringType(), True),
    StructField("Function", StringType(), True),
    StructField("Message", StringType(), True),
    StructField("any", MapType(StringType(), StringType()), True)  # For <xs:any>
])

# Define UserDataType schema
UserDataType = StructType([
    StructField("any", MapType(StringType(), StringType()), True)  # For <xs:any>
])

# Define EventDataType schema
EventDataType = StructType([
    StructField("Data", ArrayType(DataType), True),
    StructField("ComplexData", ArrayType(ComplexDataType), True),
    StructField("Binary", StringType(), True),  # hexBinary type
    StructField("_Name", StringType(), True)
])

# Define EventType schema
EventType = StructType([
    StructField("System", SystemPropertiesType, True),
    StructField("EventData", EventDataType, True),
    StructField("UserData", UserDataType, True),
    StructField("DebugData", DebugDataType, True),
    StructField("BinaryEventData", StringType(), True),  # hexBinary type
    StructField("ProcessingErrorData", ProcessingErrorDataType, True),
    StructField("RenderingInfo", RenderingInfoType, True),
    StructField("any", MapType(StringType(), StringType()), True)  # For <xs:any>
])
