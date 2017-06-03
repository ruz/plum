use lib '.';
use inc::Module::Install;

all_from('lib/Plum.pm');

requires
    'Encode' => 0,
    'JSON' => 0,
    'Carp' => 0,
;

WriteAll();
