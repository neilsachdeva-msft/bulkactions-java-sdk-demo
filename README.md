# BulkActions Demo (Java)

This project demonstrates the Azure Compute BulkActions Java SDK (`azure-resourcemanager-computebulkactions`).

## Demo Flow

1. Create resource group **BA-DEMO-JAVA-SDK-RG** (override with `DEMO_RESOURCE_GROUP`).
2. Create virtual network **BA-DEMO-VN** (override with `DEMO_VNET`).
3. Create **1K Regular** VMs using VM sizes.
4. LIST succeeded VMs by `operationStatus`.
5. ExecuteDelete **succeeded** VMs with `forceDeletion=true` and retry window of 15 minutes.
6. Create **40K Spot vCPUs** using VM attributes (vCPU 64–320, Intel, X64, excluded sizes: L64s_v3/L80s_v3).
7. After 1 minute, LIST VMs in `Creating` and delete them (if any) using ExecuteDelete.
8. Create **4 concurrent** Spot BulkActions of **10K vCPUs** each.
9. Delete resource group.

Region: **UKSouth**.

## Prerequisites

- Java 17+ installed
- Maven 3.8+ installed
- `az login` or environment-based auth for `DefaultAzureCredential`

## Environment Variables

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `AZURE_SUBSCRIPTION_ID` | Recommended | `1d04e8f1-ee04-4056-b0b2-718f5bb45b04` | Azure subscription |
| `DEMO_RESOURCE_GROUP` | Optional | `BA-DEMO-JAVA-SDK-RG` | Resource group name |
| `DEMO_VNET` | Optional | `BA-DEMO-VN` | Virtual network name |
| `DEMO_ADMIN_PASSWORD` | Required for real runs | `TestP@ssw0rd` | VM admin password |

## Run

```bash
export AZURE_SUBSCRIPTION_ID=<sub>
export DEMO_ADMIN_PASSWORD='<password>'
mvn compile exec:java
```

Or build and run as a JAR:

```bash
mvn package
java -cp "target/bulkactions-java-sdk-demo-1.0.0.jar;target/dependency/*" com.azure.bulkactionsdemo.BulkActionsDemo
```
