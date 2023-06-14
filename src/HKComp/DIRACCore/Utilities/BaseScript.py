'''
    Base class to clean up implementing scripts in DIRAC
'''

from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRAC_Exit
from DIRAC.Core.Base import Script
from DIRAC.Core.Base.Script import Script as ScriptI


class BaseScript:
    # switches is a list of 3 or 4 element tuples, where the elements are:
    #      0 : short form (single character) command line flag
    #      1 : long form (- separated words) command line flag
    #      2 : description string shown with --help
    #      3: default value to store once the item is registered
    #      4: is required

    #     By default, data for each flag are stored in self._<long>.replace('-','_')
    #     The value for each is set by the method self.set_<long>.replace('-','_'),
    #     a method which by default just blindly stores the value given but which
    #     may be overridden in derived class to have more logic.
    #     * here <long> is the long form of the flag specified in argument 1.
    #
    #     Note that in the implementation of self.main() any configured switches show
    #         up in kwargs, positional arguments are in args
    switches = []
    arguments = []
    usage = []

    def __init__(self):
        '''
        '''
        for switch in self.switches:
            self.registerSwitch(switch)
        for arg in self.arguments:
            self.registerArgument(arg)
        # Script.setUsageMessage(self.__doc__)
        usage_doc = []
        for a_usage in self.usage:
            usage_doc.append(str(a_usage))
        Script.setUsageMessage('\n'.join(usage_doc))
        Script.parseCommandLine(ignoreErrors=False)
        self.getPositionalArgs()
        self.checkSwitches()

    def __call__(self):
        gLogger.info('time to call main()')
        if hasattr(self, 'main'):
            self.main()
        else:
            gLogger.error(
                'no main method implemented, your custom logic goes there')
            DIRAC_Exit(1)
        DIRAC_Exit(0)

    def _default_set(self, name, value):
        gLogger.info(f'calling set {name} -> {repr(value)}')
        # This is so ugly, they set to '' when a flag is there with no value
        #   ... '' is logical False in python! grrrr (BHL)
        if value is '':
            value = True
        setattr(self, name, value)
        return S_OK()

    def registerArgument(self, arg):
        if len(arg) == 3:
            Script.registerArgument((arg[0] + ": " + arg[1]), mandatory=arg[2])
        else:
            Script.registerArgument((arg[0] + ": " + arg[1]), mandatory=False)

    def getPositionalArgs(self):
        values = Script.getPositionalArgs()
        for tuple in zip(self.arguments, values):
            setattr(self, str(tuple[0][0]), tuple[1])

    def registerSwitch(self, switch):
        if len(switch) < 5:
            gLogger.error(f'Missing item in switch {switch}')
            DIRAC_Exit(1)
        this_name = switch[1].replace('-', '_').rstrip(':=')

        # if there isn't a setter method, use default
        if not hasattr(self, 'set_' + this_name):
            gLogger.debug(f'configuring default setter for <{this_name}>')
            setattr(self, 'set_' + this_name,
                    lambda x: self._default_set(this_name, x))
        try:
            setter = getattr(self, 'set_' + this_name)
            getattr(self, 'set_' + this_name)(None)
        except:
            gLogger.error(f'unable to call {"set_" + this_name}')
            pass

        # shortName = switch[0].rstrip(':=')
        # shortName += ":"
        # longName = switch[1].rstrip(':=')
        # longName += "="
        Script.registerSwitch(
            switch[0], switch[1], switch[2], getattr(self, 'set_' + this_name))

        # a default value must be provided, so set it
        getattr(self, 'set_' + this_name)(switch[3])

    def checkSwitches(self):
        for switch in self.switches:
            this_name = switch[1].replace('-', '_').rstrip(':=')
            if getattr(self, this_name) == None and switch[4] == True:
                gLogger.error(f'Missing argument or option: {this_name} ({switch[2]})')
                ScriptI().showHelp(exitCode=1)
                DIRAC_Exit(1)

