import { ListObjectVersionsCommand, S3Client } from '@aws-sdk/client-s3';

export async function* listAllVersions(
   client: S3Client,
   bucket: string,
   prefix?: string
): AsyncIterable<{ Key: string; VersionId: string }> {
   let keyMarker: string | undefined,
       versionIdMarker: string | undefined;

   for (;;) {
      const resp = await client.send(new ListObjectVersionsCommand({
         Bucket: bucket,
         Prefix: prefix,
         KeyMarker: keyMarker,
         VersionIdMarker: versionIdMarker,
      }));

      for (const entry of [ ...(resp.Versions || []), ...(resp.DeleteMarkers || []) ]) {
         if (entry.Key && entry.VersionId) {
            yield { Key: entry.Key, VersionId: entry.VersionId }; // eslint-disable-line no-restricted-syntax
         }
      }

      if (!resp.IsTruncated) {
         break;
      }

      keyMarker = resp.NextKeyMarker;
      versionIdMarker = resp.NextVersionIdMarker;
   }
}
