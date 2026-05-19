import { ListMultipartUploadsCommand, S3Client } from '@aws-sdk/client-s3';

export async function* listMultipartUploads(
   client: S3Client,
   bucket: string,
   prefix?: string
): AsyncIterable<{ Key: string; UploadId: string }> {
   let keyMarker: string | undefined,
       uploadIdMarker: string | undefined;

   for (;;) {
      const resp = await client.send(new ListMultipartUploadsCommand({
         Bucket: bucket,
         Prefix: prefix,
         KeyMarker: keyMarker,
         UploadIdMarker: uploadIdMarker,
      }));

      for (const entry of (resp.Uploads || [])) {
         if (entry.Key && entry.UploadId) {
            yield { Key: entry.Key, UploadId: entry.UploadId }; // eslint-disable-line no-restricted-syntax
         }
      }

      if (!resp.IsTruncated) {
         break;
      }

      keyMarker = resp.NextKeyMarker;
      uploadIdMarker = resp.NextUploadIdMarker;
   }
}
