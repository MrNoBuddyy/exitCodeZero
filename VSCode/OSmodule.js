const os = require('node:os')
console.log(os.type());
console.log("hostname",os.hostname());
console.log("homedir",os.homedir());
console.log("relese",os.release());
console.log("platform",os.platform());