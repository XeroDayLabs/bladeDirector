create table bladeOwnership(
	id integer primary key autoincrement,
	state,
	currentOwner,
	nextOwner,
	lastKeepAlive,
	currentSnapshot
	);

create table bladeConfiguration(
	id integer primary key autoincrement,
	ownershipID unique,
    iscsiIP unique,
    bladeIP unique,
    iLOIP unique,
    iLOPort unique,
	currentlyHavingBIOSDeployed,
	currentlyBeingAVMServer,
	lastDeployedBIOS,

	foreign key (ownershipID) references bladeOwnership(id)
	);

create table VMConfiguration(
	id integer primary key autoincrement,
	ownershipID unique,
	parentBladeID,
	memoryMB,
	cpuCount,
	vmxPath,
    iscsiIP unique,
    bladeIP unique,
	VMIP,
	eth0MAC,
	eth1MAC,
	displayname,
	
	foreign key (parentBladeID ) references bladeConfiguration(id),
	foreign key (ownershipID) references bladeOwnership(id)
	);
