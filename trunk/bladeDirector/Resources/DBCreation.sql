create table bladeConfiguration(
	id integer primary key autoincrement,
    iscsiIP unique,
    bladeIP unique,
    iLOIP unique,
    iLOPort unique
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
