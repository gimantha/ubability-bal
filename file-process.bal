import ballerina/io;
import ballerina/file;
import ballerinax/java.jdbc;

public function main() returns error? {
    string path = file:getCurrentDir();
    jdbc:Client dbClient = check new (url = "jdbc:h2:file:./local-transactions/trxDB", user = "test", password = "test");
    _ = check dbClient->execute("CREATE TABLE IF NOT EXISTS TRANSACTIONSS " + "(REF VARCHAR(50), PERIOD VARCHAR(50))");
    string sourceCSV = path + "/sourceDir/trx.csv";
    stream<string[], error> csvStream = check io:fileReadCsvAsStream(sourceCSV);
    var csvRecord = csvStream.next();
    csvRecord = csvStream.next();
    boolean errorOccured = false;

    transaction {
        'transaction:onRollback(onRollbackFunc);
        'transaction:onCommit(onCommitFunc);
        while (csvRecord is record {|
                                any|error value;
                            |}) {
            string[]|error data = <string[]|error>csvRecord.value;
            if (data is error) {
                io:println("Error occured: ", data);
                errorOccured = true;
                break;
            } else {
                var result = dbClient->execute("INSERT INTO TRANSACTIONSS VALUES ('" + data[0] + "', '" + data[1] + "')");
                if (result is error) {
                    io:println("Error occured: ", result);
                    errorOccured = true;
                    break;
                }
            }
            csvRecord = csvStream.next();
        }
        var commitResult = commit;
        if (commitResult is error) {
            io:println("Failed to process CSV");
        }
    }
    stream<record { }, error> resultStream = dbClient->query("Select count(*) as total from TRANSACTIONSS");

    record {|
        record { } value;
    |}|error? result = resultStream.next();
    if (result is record {|
                      record { } value;
                  |}) {
        io:println("Total rows in TRX table : ", result.value["TOTAL"]);
    }
}


isolated function onRollbackFunc(transaction:Info info, error? cause, boolean willRetry) {

    string path = file:getCurrentDir();
    string csvPath = path + "/sourceDir/trx.csv";
    var result = file:copy(csvPath, path + "/failed");
    if (result is error) {
        io:println("Cannot copy file to failed dir");
    }
}

isolated function onCommitFunc(transaction:Info info) {
    string path = file:getCurrentDir();
    string csvPath = path + "/sourceDir/trx.csv";
    var result = file:copy(csvPath, path + "/completed");
    if (result is error) {
        io:println("Cannot copy file to completed dir");
    }
}
