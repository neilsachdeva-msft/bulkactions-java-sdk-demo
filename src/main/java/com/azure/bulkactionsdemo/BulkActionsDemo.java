package com.azure.bulkactionsdemo;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.resourcemanager.computebulkactions.ComputeBulkActionsManager;
import com.azure.resourcemanager.computebulkactions.fluent.models.LocationBasedLaunchBulkInstancesOperationInner;
import com.azure.core.management.SubResource;
import com.azure.resourcemanager.computebulkactions.models.ArchitectureType;
import com.azure.resourcemanager.computebulkactions.models.CachingTypes;
import com.azure.resourcemanager.computebulkactions.models.CapacityType;
import com.azure.resourcemanager.computebulkactions.models.ComputeProfile;
import com.azure.resourcemanager.computebulkactions.models.CpuManufacturer;
import com.azure.resourcemanager.computebulkactions.models.DeleteResourceOperationResponse;
import com.azure.resourcemanager.computebulkactions.models.DiskCreateOptionTypes;
import com.azure.resourcemanager.computebulkactions.models.DiskDeleteOptionTypes;
import com.azure.resourcemanager.computebulkactions.models.ExecuteDeleteRequest;
import com.azure.resourcemanager.computebulkactions.models.ExecutionParameters;
import com.azure.resourcemanager.computebulkactions.models.GetOperationStatusRequest;
import com.azure.resourcemanager.computebulkactions.models.GetOperationStatusResponse;
import com.azure.resourcemanager.computebulkactions.models.ImageReference;
import com.azure.resourcemanager.computebulkactions.models.LaunchBulkInstancesOperationProperties;
import com.azure.resourcemanager.computebulkactions.models.ManagedDiskParameters;
import com.azure.resourcemanager.computebulkactions.models.NetworkApiVersion;
import com.azure.resourcemanager.computebulkactions.models.NetworkProfile;
import com.azure.resourcemanager.computebulkactions.models.OperatingSystemTypes;
import com.azure.resourcemanager.computebulkactions.models.OperationState;
import com.azure.resourcemanager.computebulkactions.models.OSDisk;
import com.azure.resourcemanager.computebulkactions.models.OSProfile;
import com.azure.resourcemanager.computebulkactions.models.PriorityProfile;
import com.azure.resourcemanager.computebulkactions.models.Resources;
import com.azure.resourcemanager.computebulkactions.models.RetryPolicy;
import com.azure.resourcemanager.computebulkactions.models.StorageAccountTypes;
import com.azure.resourcemanager.computebulkactions.models.StorageProfile;
import com.azure.resourcemanager.computebulkactions.models.VMAttributes;
import com.azure.resourcemanager.computebulkactions.models.VMAttributeMinMaxDouble;
import com.azure.resourcemanager.computebulkactions.models.VMAttributeMinMaxInteger;
import com.azure.resourcemanager.computebulkactions.models.VMOperationStatus;
import com.azure.resourcemanager.computebulkactions.models.VirtualMachineNetworkInterfaceConfiguration;
import com.azure.resourcemanager.computebulkactions.models.VirtualMachineNetworkInterfaceConfigurationProperties;
import com.azure.resourcemanager.computebulkactions.models.VirtualMachineNetworkInterfaceIPConfiguration;
import com.azure.resourcemanager.computebulkactions.models.VirtualMachineNetworkInterfaceIPConfigurationProperties;
import com.azure.resourcemanager.computebulkactions.models.VirtualMachineProfile;
import com.azure.resourcemanager.computebulkactions.models.VirtualMachineType;
import com.azure.resourcemanager.computebulkactions.models.VmSizeProfile;
import com.azure.resourcemanager.network.NetworkManager;
import com.azure.resourcemanager.resources.ResourceManager;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Azure Compute BulkActions Java SDK Demo.
 *
 * Mirrors the Go SDK demo flow:
 *   1. Setup: authenticate, create resource group + virtual network
 *   2. Create 1K Regular VMs using VM sizes
 *   3. List succeeded VMs, then delete them (force delete)
 *   4. Create 40K Spot vCPUs using VM attributes
 *   5. After 1 min, list in-progress VMs and delete them
 *   6. Create 4 concurrent BulkActions of 10K Spot vCPUs each
 *   7. Delete resource group to cleanup
 */
public class BulkActionsDemo {

    private static final Logger LOG = Logger.getLogger(BulkActionsDemo.class.getName());

    // ── Configuration ──────────────────────────────────────────────────────────
    // NOTE: Replace with your subscription ID or set AZURE_SUBSCRIPTION_ID env var.
    private static final String DEFAULT_SUBSCRIPTION_ID = "1d04e8f1-ee04-4056-b0b2-718f5bb45b04";

    // Demo plan naming:
    //   BA-DEMO-{LANG}-SDK-RG (e.g., BA-DEMO-JAVA-SDK-RG-3)
    //   BA-DEMO-VN
    private static final String DEFAULT_RESOURCE_GROUP_NAME = "BA-DEMO-JAVA-SDK-RG";
    private static final String DEFAULT_VNET_NAME = "BA-DEMO-VN";

    private static final String LOCATION = "uksouth";

    // ── Runtime state ──────────────────────────────────────────────────────────
    private static String subscriptionId;
    private static String resourceGroupName;
    private static String vnetName;

    private static ComputeBulkActionsManager bulkActionsManager;
    private static ResourceManager resourceManager;
    private static NetworkManager networkManager;

    // ════════════════════════════════════════════════════════════════════════════
    // main
    // ════════════════════════════════════════════════════════════════════════════
    public static void main(String[] args) {
        try {
            run();
        } catch (Exception e) {
            LOG.severe(String.format("Demo failed: %s", extractMessage(e)));
            System.exit(1);
        }
    }

    private static void run() {
        // Set up the environment, create the clients, the resource group and virtual network
        setup();

        // 1) Create 1K VMs of Regular priority using VM Sizes.
        String baRegular1KOpId = createBulkActions(
                CapacityType.VM,                 // capacityType
                1000,                            // capacity
                VirtualMachineType.REGULAR,      // priorityType
                List.of(                         // vmSizesProfile
                        new VmSizeProfile().withName("Standard_F1s"),
                        new VmSizeProfile().withName("Standard_DS1_v2"),
                        new VmSizeProfile().withName("Standard_D2ads_v5"),
                        new VmSizeProfile().withName("Standard_D8as_v5")
                ),
                null,  // vmAttributes
                true   // waitForCompletion — wait for all VMs before listing
        );

        // 2) GET the list of succeeded VMs from the BA using LIST VMs.
        //    LIST VMs can be used for any operationStatus (Creating/Succeeded/Failed/etc.).
        List<String> succeededVMs = listVMsInBulkAction(baRegular1KOpId, VMOperationStatus.SUCCEEDED);
        LOG.info(String.format("Succeeded VMs in %s: %d", baRegular1KOpId, succeededVMs.size()));

        // 3) DELETE succeeded VMs using ExecuteDelete API (force delete) + retries.
        //    Note: This bulk delete API can be used to delete any Azure VMs and not just
        //    the ones created by Bulk Actions.
        if (!succeededVMs.isEmpty()) {
            bulkDeleteVMsInBatch(succeededVMs, true /* forceDelete */);
        } else {
            LOG.info(String.format("No succeeded VMs to delete for %s", baRegular1KOpId));
        }

        // 4) Create 40K vCPUs of Spot priority using VM Attributes.
        String baSpot40KOpId = createBulkActions(
                CapacityType.VCPU,               // capacityType
                40000,                           // capacity
                VirtualMachineType.SPOT,         // priorityType
                null,                            // vmSizesProfile
                buildVmAttributes(),             // vmAttributes
                false                            // waitForCompletion — don't wait, check in-progress VMs after 1 min
        );

        // 5) After 1 min, GET the list of VMs still in progress from the BA using LIST VMs.
        //    "Creating" indicates VMs in the process of being created/scheduled.
        LOG.info(String.format("Sleeping 1 minute before checking progress for %s...", baSpot40KOpId));
        sleep(Duration.ofMinutes(1));

        List<String> creatingVMs = listVMsInBulkAction(baSpot40KOpId, VMOperationStatus.CREATING);

        // 6) DELETE VMs still in progress using ExecuteDelete API if any (force delete).
        if (!creatingVMs.isEmpty()) {
            bulkDeleteVMsInBatch(creatingVMs, true /* forceDelete */);
        } else {
            LOG.info(String.format("No in-progress (Creating) VMs to delete for %s", baSpot40KOpId));
        }

        // 7) Create 4 concurrent BAs of 10K each.
        LOG.info("Creating 4 concurrent Spot BulkActions (10K vCPU each)...");
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(4);

        for (int i = 1; i <= 4; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    createBulkActions(
                            CapacityType.VCPU,           // capacityType
                            10000,                       // capacity
                            VirtualMachineType.SPOT,     // priorityType
                            null,                        // vmSizesProfile
                            buildVmAttributes(),         // vmAttributes
                            true                         // waitForCompletion
                    );
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, String.format("Concurrent BA #%d failed", idx), e);
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.severe("Interrupted waiting for concurrent BulkActions");
        }
        executor.shutdown();

        // 8) Delete resource group to cleanup.
        deleteResourceGroup();
    }

    /**
     * Extracts a clean error message from an exception, unwrapping common wrappers.
     */
    private static String extractMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String msg = cause.getMessage();
        if (msg != null && msg.length() > 500) {
            msg = msg.substring(0, 500) + "...";
        }
        return msg != null ? msg : cause.getClass().getSimpleName();
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Setup
    // ════════════════════════════════════════════════════════════════════════════
    private static void setup() {
        subscriptionId = env("AZURE_SUBSCRIPTION_ID", DEFAULT_SUBSCRIPTION_ID);
        resourceGroupName = env("DEMO_RESOURCE_GROUP", DEFAULT_RESOURCE_GROUP_NAME);
        vnetName = env("DEMO_VNET", DEFAULT_VNET_NAME);

        // Build credential using DefaultAzureCredential (supports az login, env vars,
        // managed identity, etc.).
        TokenCredential credential = new DefaultAzureCredentialBuilder()
                .authorityHost("https://login.microsoftonline.com/")
                .build();

        // Build an AzureProfile that targets the regional ARM endpoint.
        Map<String, String> endpoints = new HashMap<>(AzureEnvironment.AZURE.getEndpoints());
        endpoints.put("resourceManagerEndpoint", "https://" + LOCATION + ".management.azure.com/");
        AzureEnvironment customEnv = new AzureEnvironment(endpoints);
        AzureProfile profile = new AzureProfile(null, subscriptionId, customEnv);

        // HTTP pipeline policy that logs error responses (4xx/5xx) with correlation IDs
        HttpPipelinePolicy loggingPolicy = new ErrorLoggingPolicy();

        // Create managers
        bulkActionsManager = ComputeBulkActionsManager
                .configure()
                .withPolicy(loggingPolicy)
                .authenticate(credential, profile);

        resourceManager = ResourceManager
                .configure()
                .withPolicy(loggingPolicy)
                .authenticate(credential, profile)
                .withSubscription(subscriptionId);

        networkManager = NetworkManager
                .configure()
                .withPolicy(loggingPolicy)
                .authenticate(credential, profile);

        createResourceGroup();
        createVirtualNetwork();
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Resource Group
    // ════════════════════════════════════════════════════════════════════════════
    private static void createResourceGroup() {
        LOG.info(String.format("Creating resource group %s...", resourceGroupName));
        resourceManager.resourceGroups()
                .define(resourceGroupName)
                .withRegion(LOCATION)
                .create();
        LOG.info(String.format("Created resource group: %s", resourceGroupName));
    }

    private static void deleteResourceGroup() {
        LOG.info(String.format("Deleting resource group %s...", resourceGroupName));
        resourceManager.resourceGroups().beginDeleteByName(resourceGroupName);
        LOG.info(String.format("Deleted resource group: %s", resourceGroupName));
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Virtual Network
    // ════════════════════════════════════════════════════════════════════════════
    private static void createVirtualNetwork() {
        LOG.info(String.format("Creating virtual network %s...", vnetName));
        networkManager.networks()
                .define(vnetName)
                .withRegion(LOCATION)
                .withExistingResourceGroup(resourceGroupName)
                .withAddressSpace("10.1.0.0/16")
                .withSubnet("default", "10.1.0.0/18")
                .create();
        LOG.info(String.format("Created virtual network %s", vnetName));
    }

    // ════════════════════════════════════════════════════════════════════════════
    // BulkActions — Create
    // ════════════════════════════════════════════════════════════════════════════
    private static String createBulkActions(
            CapacityType capacityType,
            int capacity,
            VirtualMachineType priorityType,
            List<VmSizeProfile> vmSizesProfile,
            VMAttributes vmAttributes,
            boolean waitForCompletion) {

        String operationId = UUID.randomUUID().toString();
        LOG.info(String.format("Creating BulkActions (operationID=%s)...", operationId));

        String adminPassword = env("DEMO_ADMIN_PASSWORD", "TestP@ssw0rd");

        // Make the computer name unique per BulkActions to reduce collision risk.
        String prefix = safePrefix(operationId);

        String subnetId = String.format(
                "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/virtualNetworks/%s/subnets/default",
                subscriptionId, resourceGroupName, vnetName);

        LaunchBulkInstancesOperationProperties properties =
                new LaunchBulkInstancesOperationProperties()
                        .withCapacity(capacity)
                        .withCapacityType(capacityType)
                        .withPriorityProfile(new PriorityProfile()
                                .withType(priorityType))
                        .withVmSizesProfile(vmSizesProfile)
                        .withVmAttributes(vmAttributes)
                        .withComputeProfile(new ComputeProfile()
                                .withVirtualMachineProfile(new VirtualMachineProfile()
                                        .withStorageProfile(new StorageProfile()
                                                .withImageReference(new ImageReference()
                                                        .withPublisher("Canonical")
                                                        .withOffer("ubuntu-24_04-lts")
                                                        .withSku("server-gen1")
                                                        .withVersion("latest"))
                                                .withOsDisk(new OSDisk()
                                                        .withOsType(OperatingSystemTypes.LINUX)
                                                        .withCreateOption(DiskCreateOptionTypes.FROM_IMAGE)
                                                        .withDeleteOption(DiskDeleteOptionTypes.DELETE)
                                                        .withCaching(CachingTypes.READ_WRITE)
                                                        .withManagedDisk(new ManagedDiskParameters()
                                                                .withStorageAccountType(StorageAccountTypes.STANDARD_LRS))))
                                        .withOsProfile(new OSProfile()
                                                .withComputerName(prefix)
                                                .withAdminUsername("sample-user")
                                                .withAdminPassword(adminPassword))
                                        .withNetworkProfile(new NetworkProfile()
                                                .withNetworkApiVersion(NetworkApiVersion.TWO_ZERO_TWO_ZERO_ONE_ONE_ZERO_ONE)
                                                .withNetworkInterfaceConfigurations(List.of(
                                                        new VirtualMachineNetworkInterfaceConfiguration()
                                                                .withName("nic")
                                                                .withProperties(new VirtualMachineNetworkInterfaceConfigurationProperties()
                                                                        .withPrimary(true)
                                                                        .withEnableIPForwarding(true)
                                                                        .withIpConfigurations(List.of(
                                                                                new VirtualMachineNetworkInterfaceIPConfiguration()
                                                                                        .withName("ip")
                                                                                        .withProperties(new VirtualMachineNetworkInterfaceIPConfigurationProperties()
                                                                                                .withSubnet(new SubResource().withId(subnetId))
                                                                                                .withPrimary(true))))))))));

        if (waitForCompletion) {
            // Use the fluent define/create pattern which waits for the LRO to complete.
            bulkActionsManager.bulkActions()
                    .define(operationId)
                    .withExistingLocation(resourceGroupName, LOCATION)
                    .withProperties(properties)
                    .create();
            LOG.info(String.format("BulkActions completed (operationID=%s)", operationId));
        } else {
            // Start the LRO but don't wait for it to finish.
            // Use the inner fluent client's beginCreateOrUpdate which returns a SyncPoller
            // that we don't poll — giving us fire-and-forget behavior.
            try {
                LocationBasedLaunchBulkInstancesOperationInner innerResource =
                        new LocationBasedLaunchBulkInstancesOperationInner()
                                .withProperties(properties);

                bulkActionsManager.serviceClient().getBulkActions()
                        .beginCreateOrUpdate(resourceGroupName, LOCATION, operationId, innerResource);
                LOG.info(String.format("BulkActions started (operationID=%s), not waiting for completion", operationId));
            } catch (Exception e) {
                LOG.log(Level.SEVERE, String.format("Failed to start BulkActions %s", operationId), e);
                throw new RuntimeException(e);
            }
        }

        return operationId;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // BulkActions — List VMs
    // ════════════════════════════════════════════════════════════════════════════
    private static List<String> listVMsInBulkAction(String operationId, VMOperationStatus statusFilter) {
        List<String> vmIds = new ArrayList<>();

        // Fetch all VMs (no server-side $filter) and filter client-side,
        // because the API does not support filtering by OperationStatus.
        bulkActionsManager.bulkActions()
                .listVirtualMachines(resourceGroupName, LOCATION, operationId)
                .forEach(vm -> {
                    if (statusFilter != null) {
                        if (vm.operationStatus() == null || !vm.operationStatus().equals(statusFilter)) {
                            return; // skip non-matching VMs
                        }
                    }
                    if (vm.id() != null) {
                        vmIds.add(vm.id());
                    }
                });

        String statusStr = statusFilter != null ? statusFilter.toString() : "<all>";
        LOG.info(String.format("Total VMs found in BulkActions %s (status=%s): %d", operationId, statusStr, vmIds.size()));
        return vmIds;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Bulk Delete — Batched
    // ════════════════════════════════════════════════════════════════════════════
    private static void bulkDeleteVMsInBatch(List<String> vmIds, boolean forceDelete) {
        if (vmIds.isEmpty()) {
            LOG.info("No VMs to delete");
            return;
        }

        List<List<String>> batches = batch(vmIds, 100);
        // Limit to 3 concurrent delete requests to avoid subscription-level throttling (429).
        Semaphore semaphore = new Semaphore(3);
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(batches.size(), 3));
        CountDownLatch latch = new CountDownLatch(batches.size());

        for (int i = 0; i < batches.size(); i++) {
            final int idx = i;
            final List<String> batchIds = batches.get(i);
            executor.submit(() -> {
                try {
                    semaphore.acquire();
                    LOG.info(String.format("Deleting batch %d/%d (%d VMs)...", idx + 1, batches.size(), batchIds.size()));
                    bulkDeleteVMs(batchIds, forceDelete);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.severe("Interrupted during batch delete");
                } finally {
                    semaphore.release();
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.severe("Interrupted waiting for batch deletes");
        }
        executor.shutdown();
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Bulk Delete — Single Batch
    // ════════════════════════════════════════════════════════════════════════════
    private static void bulkDeleteVMs(List<String> vmIds, boolean forceDelete) {
        LOG.info(String.format("Starting ExecuteDelete for %d VMs (force=%b)", vmIds.size(), forceDelete));

        String correlationId = UUID.randomUUID().toString();

        // Start deletion with retry on 429 throttling.
        // NOTE: This API can delete ANY Azure VMs given their ARM IDs.
        List<String> operationIds = new ArrayList<>();

        for (int attempt = 1; attempt <= 5; attempt++) {
            try {
                DeleteResourceOperationResponse resp = bulkActionsManager.bulkActions()
                        .virtualMachinesExecuteDelete(LOCATION, new ExecuteDeleteRequest()
                                .withExecutionParameters(new ExecutionParameters()
                                        .withRetryPolicy(new RetryPolicy()
                                                .withRetryCount(5)
                                                .withRetryWindowInMinutes(15)))
                                .withResources(new Resources().withIds(vmIds))
                                .withCorrelationId(correlationId)
                                .withForceDeletion(true));

                // Extract operation IDs from the response results
                if (resp.results() != null) {
                    resp.results().forEach(result -> {
                        if (result.operation() != null && result.operation().operationId() != null) {
                            operationIds.add(result.operation().operationId());
                        }
                    });
                }
                break; // success
            } catch (Exception e) {
                if (attempt == 5) {
                    LOG.severe(String.format("ExecuteDelete failed after 5 attempts (corr=%s): %s", correlationId, e.getMessage()));
                    throw new RuntimeException(e);
                }
                long backoff = attempt * 15L;
                LOG.warning(String.format("ExecuteDelete throttled (attempt %d/5), retrying in %ds: %s", attempt, backoff, e.getMessage()));
                sleep(Duration.ofSeconds(backoff));
            }
        }

        if (operationIds.isEmpty()) {
            LOG.warning(String.format("No operation IDs returned from ExecuteDelete (corr=%s)", correlationId));
            return;
        }

        // Poll until terminal state (Succeeded/Failed/Canceled).
        Instant deadline = Instant.now().plus(Duration.ofMinutes(60));

        while (Instant.now().isBefore(deadline)) {
            sleep(Duration.ofSeconds(60));

            try {
                GetOperationStatusResponse statusResp = bulkActionsManager.bulkActions()
                        .virtualMachinesGetOperationStatus(LOCATION, new GetOperationStatusRequest()
                                .withOperationIds(operationIds)
                                .withCorrelationId(correlationId));

                boolean allDone = true;
                int failedCount = 0;

                if (statusResp.results() != null) {
                    for (var result : statusResp.results()) {
                        if (result.operation() == null || result.operation().state() == null) {
                            allDone = false;
                            break;
                        }
                        OperationState state = result.operation().state();
                        if (OperationState.SUCCEEDED.equals(state)) {
                            // ok
                        } else if (OperationState.FAILED.equals(state)
                                || OperationState.CANCELLED.equals(state)) {
                            failedCount++;
                        } else {
                            // InProgress, Queued, etc.
                            allDone = false;
                            break;
                        }
                    }
                }

                if (allDone) {
                    if (failedCount > 0) {
                        LOG.warning(String.format("ExecuteDelete completed with %d failed operations (corr=%s)", failedCount, correlationId));
                    } else {
                        LOG.info(String.format("ExecuteDelete completed successfully (corr=%s)", correlationId));
                    }
                    return;
                }
            } catch (Exception e) {
                // Transient errors (including 429 throttling) — log and retry on next tick.
                LOG.warning(String.format("Polling error (will retry on next tick): %s", e.getMessage()));
            }
        }

        LOG.severe(String.format("Timed out polling delete operations (corr=%s)", correlationId));
        throw new RuntimeException("Timed out polling delete operations");
    }

    // ════════════════════════════════════════════════════════════════════════════
    // VM Attributes (for Spot VMs with attribute-based sizing)
    // ════════════════════════════════════════════════════════════════════════════
    private static VMAttributes buildVmAttributes() {
        return new VMAttributes()
                .withVCpuCount(new VMAttributeMinMaxInteger()
                        .withMin(64)
                        .withMax(320))
                .withMemoryInGiB(new VMAttributeMinMaxDouble()
                        .withMin(100.0)
                        .withMax(3000.0))
                .withMemoryInGiBPerVCpu(new VMAttributeMinMaxDouble()
                        .withMin(8.0))
                .withArchitectureTypes(List.of(ArchitectureType.X64))
                .withCpuManufacturers(List.of(CpuManufacturer.INTEL))
                .withExcludedVMSizes(List.of(
                        "Standard_L64s_v3",
                        "Standard_L80s_v3"));
    }

    // ════════════════════════════════════════════════════════════════════════════
    // HTTP Error Logging Policy
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * HTTP pipeline policy that logs error responses (4xx/5xx) with correlation IDs
     * and response bodies for debugging, while suppressing noise from successful calls.
     * Equivalent to the Go demo's loggingPolicy.
     */
    private static class ErrorLoggingPolicy implements HttpPipelinePolicy {
        @Override
        public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
            return next.process()
                    .flatMap(response -> {
                        int statusCode = response.getStatusCode();
                        if (statusCode >= 400) {
                            String url = context.getHttpRequest().getUrl().toString();
                            String method = context.getHttpRequest().getHttpMethod().toString();
                            String corrId = response.getHeaders().getValue(
                                    HttpHeaderName.fromString("x-ms-correlation-request-id"));

                            return response.getBodyAsString()
                                    .defaultIfEmpty("")
                                    .map(body -> {
                                        LOG.warning(String.format("[HTTP] %d %s %s | Correlation-ID: %s",
                                                statusCode, method, url, corrId));
                                        if (!body.isEmpty()) {
                                            LOG.warning(String.format("[HTTP] Response Body: %s", body));
                                        }
                                        return response.buffer();
                                    });
                        }
                        return Mono.just(response);
                    });
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Helpers
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * Generates a safe computer name prefix from the given name.
     * Keeps alphanumerics and dashes, truncates to 10 chars, appends a 4-char UUID suffix.
     */
    private static String safePrefix(String name) {
        String cleaned = name.replaceAll("[^a-zA-Z0-9-]", "").toLowerCase();
        if (cleaned.isEmpty()) {
            cleaned = "ba";
        }
        if (cleaned.length() > 10) {
            cleaned = cleaned.substring(0, 10);
        }
        String suffix = UUID.randomUUID().toString().substring(0, 4);
        return cleaned + "-" + suffix;
    }

    /**
     * Partitions a list into sublists of the specified batch size.
     */
    private static <T> List<List<T>> batch(List<T> input, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < input.size(); i += batchSize) {
            int end = Math.min(i + batchSize, input.size());
            batches.add(new ArrayList<>(input.subList(i, end)));
        }
        return batches;
    }

    /**
     * Reads an environment variable with a default fallback.
     */
    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }

    /**
     * Sleeps for the given duration, handling interruption.
     */
    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
