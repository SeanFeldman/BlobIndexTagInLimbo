# Blob Index Tag in limbo state

To repro, have an Azure Storage connection string defined in the user secrets folder `BlobIndexTagInLimbo` as a setting called `AzureStorageConnectionString`.
Run the steps to reproduce the issue. The following steps are defined as unit tests:

![image](https://user-images.githubusercontent.com/1309622/117766766-4e107900-b1ed-11eb-96bc-95588bc23da4.png)

## Hypothesis

What it looks like is that when a blob is copied w/o a workaround (Storage SDK issue https://github.com/Azure/azure-sdk-for-net/issues/20931), the blob attempted with tag and tag conditions is failing to be copied. The operations seems to be non atomic and the search index (assuming this is how it works) is updated with blob name but the actual blob is not created. Subsequent attempt to upload the blob fail with error code 412 indicating there's a newer blob while there's no blob at all.
