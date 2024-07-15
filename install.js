var Service = require('node-windows').Service;

console.log('========ContextManager will be installed as Windows service=========');

if(process.argv.length === 2 || process.argv.length !== 5 )
{
	console.log('Pass arguments as given here under quotation marks--> "<name of service>" "<description of service>" "<path of nodejs file>"');
	return;
}

var svcName = process.argv[2];
var svcDesc = process.argv[3];
var svcPath = process.argv[4];

console.log('ServiceName: ', svcName, 'Path:', svcPath);

// Create a new service object
var svc = new Service({
  name:svcName,
  description: svcDesc,
  script: svcPath
});

// Listen for the "install" event, which indicates the
// process is available as a service.
svc.on('install',function(){
  svc.start();
  console.log('Service (', svcName, ') installed successfully !');
  
});

svc.install();

svc.on('stop',function(){
  svc.stop();
  console.log('Service (', svcName, ') stopped successfully !');
  
});

svc.stop();


