create table bladeOwnership(
	ownershipKey integer primary key autoincrement,
	state,
	currentOwner,
	nextOwner,
	lastKeepAlive,
	currentSnapshot,
	friendlyName unique,
    kernelDebugPort unique,
    kernelDebugKey,
	availableUsersCSV
	);

create table bladeConfiguration(
	bladeConfigKey integer primary key autoincrement,
	ownershipID unique,
    iscsiIP unique,
    bladeIP unique,
    iLOIP unique,
	currentlyHavingBIOSDeployed,
	currentlyBeingAVMServer,
	vmDeployState,
	lastDeployedBIOS,

	foreign key (ownershipID) references bladeOwnership(ownershipKey)
	);

create table VMConfiguration(
	vmConfigKey integer primary key autoincrement,
	indexOnServer,
	ownershipID unique,
	parentBladeID,
	parentBladeIP,
	memoryMB,
	cpuCount,
	vmxPath,
    iscsiIP unique,
	VMIP,
	eth0MAC,
	eth1MAC,
	isWaitingForResources,

	foreign key (parentBladeID ) references bladeConfiguration(bladeConfigKey),
	foreign key (ownershipID) references bladeOwnership(ownershipKey)
	);
