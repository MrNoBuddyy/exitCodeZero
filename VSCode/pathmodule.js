const path =require('path');
const a=path.basename('C:\\temp\\myfile.html');
const a1=path.dirname('C:\\temp\\myfile.html');
console.log(a);
console.log(a1);
const b=path.extname(__filename);
console.log(b);
console.log(__filename,b)