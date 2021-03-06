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

const verbose = true;      // Print verbose messages
const maxFilesToCopy = 100; // maxium number of files to copy per execution

// Array of all the paths to sync.  The basename of the path is the remote container name.
const ContainerPath = [
    //"/tmp/JKH",
    //"/tmp/JKH2"
 
    "/mnt/snap2/workfiles/webFiles/webPDFs",
    "/mnt/snap2/workfiles/webFiles/issuuPDFs",
    "/mnt/snap2/workfiles/webFiles/mobileAds",
    "/mnt/snap2/workfiles/webFiles/webResources",
    "/mnt/snap2/workfiles/webFiles/mobileResources",
    "/mnt/snap2/workfiles/webFiles/puzzleAnswers",
    "/mnt/snap2/workfiles/webFiles/communityPhotos",
    "/mnt/snap2/workfiles/webFiles/communityAds",
    "/mnt/snap2/workfiles/webFiles/loweDown",

];

const skipFilesStartingWith = [
    "."
];
 
// Only sync files > checkDate
const checkDate = new Date();
// Set checkDate to 13 months prior to current date
checkDate.setMonth(checkDate.getMonth() - 13);

// Nothing below here should need to be changed

let filesCopied = 0;

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

function copyFile(myContainer, localFile, remoteFile, msg){
    
    // Copies localFile to myContainer/remoteFile

    // Create a read stream for our source file
    var source = fs.createReadStream(localFile);

    // Create a writeable stream for our destination
    var dest = client.upload({
        container: myContainer,
        remote: remoteFile
    });

    dest.on('error', function(err) {
        console.log(`${filesCopied} - copied ${msg} file: ${localFile}`);
    });

    dest.on('success', function(file) {
        console.log(`Error copying ${msg} file: ${localFile}`)
    });

    filesCopied++;

    // pipe the source to the destination
    source.pipe(dest);
}

function copyFiles(localPath, myContainer, localFiles, remoteFiles){
    console.log(`Container: '${myContainer}'`);
    verboseLog(`Remote file count: ${remoteFiles.length}`);

    let count = 0;
    verboseLog(`Local file count: ${localFiles.length}`);

    console.log(`${localFiles.length} local files`);

    // Loop through all our local files to see which need to be synced
    let fileLoop = 0;
    while (fileLoop < localFiles.length && filesCopied <= maxFilesToCopy)
    {
        if ( ++count % 5000 == 0){
            console.log(`Processed ${count}`)
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
                        copyFile(client, myContainer, fqp, fileName, "modified");
                    }
                    else{
                        verboseLog(`   md5 same so skipping`);
                    }
                }
                else{
                    // File not in remote container so copy it over
                    copyFile(client, myContainer, fqp, fileName, "new");
                }
            }
            else{
                verboseLog(`   skipping because of date`);
            }
        }

        fileLoop++;
    }

    console.log(`Done.`);
}

function getRemoteFiles(localPath, myContainer, localFiles){

    return new Promise(resolve => {
        // Retrieve array 'remoteFiles' of all remote files in container
        client.getFiles(myContainer, options, function(err, remoteFiles){
            if (err){
                console.log(`Error with remote container: ${myContainer}`);
                resolve();
            }
            else
            {
                copyFiles(localPath, myContainer, localFiles, remoteFiles);
                resolve();
            }
        });
    });
}

async function readDirectory(localPath, myContainer){

     // Get array 'items' all all local files in directory

     verboseLog(`Local path set to: ${localPath}`);

     if (fs.existsSync(localPath)) {
         localFiles = fs.readdirSync(localPath);

         for (var loop = 0; loop < localFiles.length; loop++){
             var x = await getRemoteFiles(localPath, myContainer, localFiles);
             loop++;
         }
     }
     else{
         console.log(`Directory does not exist: ${localPath}`);
     }
}

function verboseLog(msg){
    // prints verbose messages when verbose=true
    if (verbose){
        console.log(msg);
    }
}


// Loop through each directory we want to sync
async function Go(){
    for (var containerLoop=0; containerLoop < ContainerPath.length; containerLoop++) {
        const localPath = ContainerPath[containerLoop];
        const myContainer = path.basename(localPath);
        
        if ( filesCopied <= maxFilesToCopy )
        {
            var x = await readDirectory(localPath, myContainer);
        }
    }
}

Go();