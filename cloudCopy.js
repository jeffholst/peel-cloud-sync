var fs = require('fs');
const path = require('path');
pkgcloud = require('pkgcloud');
const md5File = require('md5-file');

// Reference
// https://github.com/pkgcloud/pkgcloud/blob/master/docs/providers/rackspace/storage.md

/*
  _   __         _      __   __      
 | | / /__ _____(_)__ _/ /  / /__ ___
 | |/ / _ `/ __/ / _ `/ _ \/ / -_|_-<
 |___/\_,_/_/ /_/\_,_/_.__/_/\__/___/
                                     
*/

const verbose = false;   // Print verbose messages

// Array of all the paths to sync.  The basename of the path is the remote container name.
const ContainerPath = [
    //"/tmp/JKH"
    "/mnt/snap2/workfiles/webFiles/webPDFs",
    "/mnt/snap2/workfiles/webFiles/issuuPDFs"
];

const skipFilesStartingWith = [
    "."
];
 
// Only sync files > checkDate
const checkDate = new Date();
// Set checkDate to 13 months prior to current date
checkDate.setMonth(checkDate.getMonth() - 13);

// Nothing below here should need to be changed

// Client credentials
var client = pkgcloud.storage.createClient({
  provider: 'rackspace',
  username: 'capeel',
  apiKey: 'a8d8c3b44a1758f74aa94ce72e6d5391',
  region: 'DFW'
});

// Options to use when retreiving remote files
var options = {
    limit: Infinity // Infinity = all files, otherwise use integer
};

function copyFile(cliient, myContainer, localFile, remoteFile){
    // Copies localFile to myContainer/remoteFile

    // Create a read stream for our source file
    var source = fs.createReadStream(localFile);

    // Create a writeable stream for our destination
    var dest = client.upload({
        container: myContainer,
        remote: remoteFile
    });

    dest.on('error', function(err) {
        // TODO handle err as appropriate
    });

    dest.on('success', function(file) {
        // TODO handle successful upload case
    });

    // pipe the source to the destination
    source.pipe(dest);
}

function verboseLog(msg){
    // prints verbose messages when verbose=true
    if (verbose){
        console.log(msg);
    }
}

let count;
// Loop through each directory we want to sync
for (var containerLoop=0; containerLoop < ContainerPath.length; containerLoop++) {
    const localPath = ContainerPath[containerLoop];
    const myContainer = path.basename(localPath);
    verboseLog(`Local path set to: ${localPath}`);

    // Retrieve array 'remoteFiles' of all remote files in container
    client.getFiles(myContainer, options, function(err, remoteFiles){
        console.log(`Container: '${myContainer}'`);
        verboseLog(`Remote file count: ${remoteFiles.length}`);

        // Get array 'items' all all local files in directory
        fs.readdir(localPath, function(err, localFiles) {
            count = 0;
            verboseLog(`Local file count: ${localFiles.length}`);

            // Loop through all our local files to see which need to be synced
            for (var fileLoop=0; fileLoop < localFiles.length; fileLoop++) {
                if ( ++count % 500 == 0){
                    console.log(`Processed ${count} of ${localFiles.length}`)
                }

                fileName = localFiles[fileLoop];
                fqp = path.join(localPath, fileName);

                verboseLog(`File ${fileLoop + 1} of ${localFiles.length}`);
                verboseLog(`   full path: ${fqp}`);
                verboseLog(`   filename: ${fileName}`);
                
                let skip = false;
                let skipFilesLoop = 0;

                var stats = fs.statSync(fqp);

                if (stats.isDirectory()){
                    skip = true;
                    verboseLog(`   skipping directory`);
                }

                while(!skip && skipFilesLoop < skipFilesStartingWith.length){
                    if (fileName.startsWith(skipFilesStartingWith[skipFilesLoop])){
                        skip = true;
                        verboseLog(`   skipping: ${skipFilesStartingWith[skipFilesLoop]}`);
                    }

                    skipFilesLoop++;
                }

                if (!skip){
                    let localFileModDate = new Date(stats["mtime"]);
                        
                    verboseLog(`   modified date: ${localFileModDate}`);

                    if ( localFileModDate > checkDate){
                        remoteFile = remoteFiles.find(function(f){return f.name === fileName});

                        if (remoteFile){
                            verboseLog(`   file found in remote container`);
                            const hash = md5File.sync(fqp);

                            if (remoteFile.etag != hash){
                                console.log(`   Copying changed file: ${fileName}`);
                                copyFile(client, myContainer, fqp, fileName);
                            }
                            else{
                                verboseLog(`   md5 same so skipping`);
                            }
                        }
                        else{
                            // File not in remote container so copy it over
                            console.log(`   Copying new file: ${fileName}`);
                            copyFile(client, myContainer, fqp, fileName);
                        }
                    }
                    else{
                        verboseLog(`   skipping because of date`);
                    }
                }
            }
        });
    });
}
