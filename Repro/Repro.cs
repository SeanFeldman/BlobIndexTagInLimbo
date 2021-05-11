using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
using Xunit;
using Xunit.Abstractions;


public class Repro
{
    private readonly ITestOutputHelper  output;
    private readonly string connectionString;

    public Repro(ITestOutputHelper  output)
    {
        this.output = output;
        
        var configuration = new ConfigurationBuilder()
            .AddEnvironmentVariables()
            .AddUserSecrets<Repro>()
            .Build();

        connectionString = configuration["AzureStorageConnectionString"];
    }
    
    [Fact]
    public async Task Step0_optional_reset()
    {
        var serviceClient = new BlobServiceClient(connectionString);
        var containerClient = serviceClient.GetBlobContainerClient("repro");
        
        await containerClient.DeleteIfExistsAsync();
    }
    
    
    [Fact]
    public async Task Step1_prepare_source_blob()
    {
        var serviceClient = new BlobServiceClient(connectionString);
        var containerClient = serviceClient.GetBlobContainerClient("repro");
        
        await containerClient.CreateIfNotExistsAsync();

        var blobClient = containerClient.GetBlobClient("aabbccdd-1122-3344-5566-778899aabbcc.txt");
        await using var content = new MemoryStream(Encoding.UTF8.GetBytes("Original content"));

        await blobClient.UploadAsync(content);
        output.WriteLine("Blob 'repo/aabbccdd-1122-3344-5566-778899aabbcc.txt' has been created.");
    }
    
    [Fact]
    public async Task Step2_cause_blob_to_end_up_in_a_limbo_state_by_moving_it()
    {
        // Download the blob
        var serviceClient = new BlobServiceClient(connectionString);
        var containerClient = serviceClient.GetBlobContainerClient("repro");
        var blobClient = containerClient.GetBlobClient("aabbccdd-1122-3344-5566-778899aabbcc.txt");

        var response = await blobClient.DownloadAsync();
        
        // upload to the destination - first two characters and then the original blob
        var uploadClient = containerClient.GetBlobClient("aa/aabbccdd-1122-3344-5566-778899aabbcc.txt");
        
        // The code set the LocalId in case this is an insert and verifies the blob tag value is lesser value in case it's an update 
        var options = new BlobUploadOptions
        {
            Tags = new Dictionary<string, string> { { "LocalId", "123" } },
            Conditions = new BlobRequestConditions
            {
                TagConditions = $@"""LocalId"" < '123'"
            }
        };

        try
        {
            await uploadClient.UploadAsync(response.Value.Content, options);
        }
        catch (RequestFailedException exception) when(exception.ErrorCode == BlobErrorCode.ConditionNotMet)
        {
            output.WriteLine("The blob 'repo/aa/aabbccdd-1122-3344-5566-778899aabbcc.txt' is now a limbo state.");
        }
    }
    
    [Fact]
    public async Task Step3_attempt_to_upload_a_new_version_of_blob_using_stream_workaround_that_works()
    {
        // Download the blob
        var serviceClient = new BlobServiceClient(connectionString);
        var containerClient = serviceClient.GetBlobContainerClient("repro");
        var blobClient = containerClient.GetBlobClient("aabbccdd-1122-3344-5566-778899aabbcc.txt");

        var response = await blobClient.DownloadAsync();
        
        // upload to the destination - first two characters and then the original blob
        var uploadClient = containerClient.GetBlobClient("aa/aabbccdd-1122-3344-5566-778899aabbcc.txt");
        
        // The code set the LocalId in case this is an insert and verifies the blob tag value is lesser value in case it's an update 
        var options = new BlobUploadOptions
        {
            Tags = new Dictionary<string, string> { { "LocalId", "456" } },
            Conditions = new BlobRequestConditions
            {
                TagConditions = $@"""LocalId"" < '456'"
            }
        };
         
        // Workaround for https://github.com/Azure/azure-sdk-for-net/issues/20931
        await using var memoryStreamFix = new MemoryStream();
        await response.Value.Content.CopyToAsync(memoryStreamFix).ConfigureAwait(false);
        memoryStreamFix.Position = 0;
        // End Workaround

        try
        {
            await uploadClient.UploadAsync(memoryStreamFix, options);
        }
        catch (RequestFailedException exception) when(exception.ErrorCode == BlobErrorCode.ConditionNotMet)
        {
            output.WriteLine("Failed to copy 'repo/aa/aabbccdd-1122-3344-5566-778899aabbcc.txt' to 'repo/aa/aabbccdd-1122-3344-5566-778899aabbcc.txt' despite workaround.");
            output.WriteLine("Reason: 'repo/aa/aabbccdd-1122-3344-5566-778899aabbcc.txt' reported as existing and condition not met (412) despite 'repo/aa/aabbccdd-1122-3344-5566-778899aabbcc.txt' absense.");
            output.WriteLine($"Exception: {exception.Message}");
        }
    }
    
    [Fact]
    public async Task Step4_remove_blob_from_the_limbo_state()
    {
        // Download the blob
        var serviceClient = new BlobServiceClient(connectionString);
        var containerClient = serviceClient.GetBlobContainerClient("repro");
        var blobClient = containerClient.GetBlobClient("aabbccdd-1122-3344-5566-778899aabbcc.txt");

        var response = await blobClient.DownloadAsync();
        
        // upload to the destination - first two characters and then the original blob
        var uploadClient = containerClient.GetBlobClient("aa/aabbccdd-1122-3344-5566-778899aabbcc.txt");
        
        // do not set index tax + condition == plain blob
         
        // Workaround for https://github.com/Azure/azure-sdk-for-net/issues/20931
        await using var memoryStreamFix = new MemoryStream();
        await response.Value.Content.CopyToAsync(memoryStreamFix).ConfigureAwait(false);
        memoryStreamFix.Position = 0;
        // End Workaround

        await uploadClient.UploadAsync(memoryStreamFix, true);
        output.WriteLine("Blob 'repo/aa/aabbccdd-1122-3344-5566-778899aabbcc.txt' has been created and step #3 can be executed w/o exceptions.");
    }
}
