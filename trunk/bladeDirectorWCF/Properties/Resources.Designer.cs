﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace bladeDirectorWCF.Properties {
    using System;
    
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "4.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    internal class Resources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal Resources() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("bladeDirectorWCF.Properties.Resources", typeof(Resources).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///   Overrides the current thread's CurrentUICulture property for all
        ///   resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to #!/bin/sh
        ///
        ///cd ~/
        ///biosFilename=newbios.xml
        ///
        ///echo Applying BIOS from $biosFilename
        ///
        ///chmod 755 conrep
        ///echo executing ./conrep -l -f $biosFilename
        ///./conrep -l -f $biosFilename
        ///errCode=$?
        ///echo conrep returned $errCode
        ///
        ///exit $errCode
        ///.
        /// </summary>
        internal static string applyBIOS {
            get {
                return ResourceManager.GetString("applyBIOS", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized resource of type System.Byte[].
        /// </summary>
        internal static byte[] conrep {
            get {
                object obj = ResourceManager.GetObject("conrep", resourceCulture);
                return ((byte[])(obj));
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to &lt;Conrep&gt;
        ///	&lt;global&gt;
        ///		&lt;helptext&gt;&lt;![CDATA[
        ///
        ///		This is the input file for CONREP describing the 
        ///		configurable settings for the ProLiant servers listed below.
        ///		Other models require platform specific configuration files that are available
        ///		on hp.com.
        ///
        ///		]]&gt;&lt;/helptext&gt;
        ///	&lt;fileversion&gt;4.35&lt;/fileversion&gt;
        ///	&lt;minimumconrepversion&gt;3.40&lt;/minimumconrepversion&gt;
        ///	&lt;platforms&gt;
        ///		&lt;platform&gt;ProLiant DL120 G7&lt;/platform&gt;
        ///		&lt;platform&gt;ProLiant ML110 G7&lt;/platform&gt;
        ///		&lt;platform&gt;ProLiant DL3&lt;/platform&gt;
        ///		&lt;platform&gt;ProLiant ML3&lt;/pla [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string conrep_xml {
            get {
                return ResourceManager.GetString("conrep_xml", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to create table bladeOwnership(
        ///	ownershipKey integer primary key autoincrement,
        ///	state,
        ///	VMDeployState,
        ///	currentOwner,
        ///	nextOwner,
        ///	lastKeepAlive,
        ///	currentSnapshot
        ///	);
        ///
        ///create table bladeConfiguration(
        ///	bladeConfigKey integer primary key autoincrement,
        ///	ownershipID unique,
        ///    iscsiIP unique,
        ///    bladeIP unique,
        ///    iLOIP unique,
        ///    iLOPort unique,
        ///	currentlyHavingBIOSDeployed,
        ///	currentlyBeingAVMServer,
        ///	vmDeployState,
        ///	lastDeployedBIOS,
        ///
        ///	foreign key (ownershipID) references bladeOwner [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string DBCreation {
            get {
                return ResourceManager.GetString("DBCreation", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to #!/bin/sh
        ///
        ///cd ~/
        ///biosFilename=currentbios.xml
        ///
        ///chmod 755 conrep
        ///./conrep -s -f $biosFilename
        ///errCode=$?
        ///
        ///exit $errCode
        ///.
        /// </summary>
        internal static string getBIOS {
            get {
                return ResourceManager.GetString("getBIOS", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to #!ipxe
        ///
        ///ifclose net0
        ///ifopen net1
        ///set net1/ip {BLADE_IP_ISCSI}
        ///set net1/netmask {BLADE_NETMASK_ISCSI}
        ///set net1/gateway 0.0.0.0
        ///set keep-san 1
        ///set initiator-iqn iqn.2017.05.lan.xd.fuzz:${mac:hexhyp}
        ///sanboot iscsi:192.168.191.254::::iqn.2016-06.lan.xd.store:{BLADE_IP_MAIN}-{BLADE_OWNER}-{BLADE_SNAPSHOT}
        ///.
        /// </summary>
        internal static string ipxeTemplate {
            get {
                return ResourceManager.GetString("ipxeTemplate", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to #!ipxe
        ///
        ///set use-cached 1
        ///dhcp net0
        ///set net0/209:string pxelinux.cfg/AC1181
        ///set net0/210:string tftp://172.17.191.253/ltsp/amd64/
        ///# These two come from the original (windows) tftpd, hence the slashes
        ///#imgload boot\x86\pxelinux.0
        ///#boot boot\x86\pxelinux.0
        ///imgload tftp://172.17.191.253/ltsp/amd64/pxelinux.0
        ///boot tftp://172.17.191.253/ltsp/amd64/pxelinux.0
        ///.
        /// </summary>
        internal static string ipxeTemplateForBIOS {
            get {
                return ResourceManager.GetString("ipxeTemplateForBIOS", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to #!ipxe
        ///
        ///set 210 boot\x86\local\esxi\/
        ///chain tftp://172.17.191.254/\boot\x86\local\esxi\pxelinux.0.
        /// </summary>
        internal static string ipxeTemplateForESXi {
            get {
                return ResourceManager.GetString("ipxeTemplateForESXi", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to &lt;?xml version=&quot;1.0&quot; encoding=&quot;UTF-8&quot;?&gt;
        ///&lt;!--generated by conrep version 3.40--&gt;
        ///&lt;Conrep version=&quot;3.40&quot; originating_platform=&quot;ProLiant SL390s G7&quot; originating_family=&quot;P69&quot; originating_romdate=&quot;01/30/2011&quot; originating_processor_manufacturer=&quot;Intel&quot;&gt;
        ///  &lt;Section name=&quot;IMD_ServerName&quot; helptext=&quot;LCD Display name for this server&quot;&gt;
        ///    &lt;Line0&gt;BL390Z&lt;/Line0&gt;
        ///  &lt;/Section&gt;
        ///  &lt;Section name=&quot;IPL_Order&quot; helptext=&quot;Current Initial ProgramLoad device boot order.&quot;&gt;
        ///    &lt;Index0&gt;04 &lt;/Index0&gt;
        ///    &lt;Index1&gt;00 &lt;/Index1&gt;
        ///    &lt;Index2&gt; [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string VMServerBIOS {
            get {
                return ResourceManager.GetString("VMServerBIOS", resourceCulture);
            }
        }
    }
}
