create table bladeConfiguration(
	id integer primary key autoincrement,
    iscsiIP unique,
    bladeIP unique,
	currentSnapshot,
    iLOIP unique,
    iLOPort unique,
	currentlyHavingBIOSDeployed,
	lastDeployedBIOS
	);

create table bladeOwnership(
	id integer primary key autoincrement,
	bladeConfigID,
	state,
	currentOwner,
	nextOwner,
	lastKeepAlive,

	foreign key (bladeConfigID) references bladeConfigurations(id)
	);
