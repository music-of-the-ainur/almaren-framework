#!/usr/bin/env perl™

use strict;
use warnings;
use feature 'say';

my $release = "0.2.7";

{
    my @majors = grep {/\w+/} map {/spark\-(\d\.\d\.\d)/;$1 || ""} qx/git branch -r/;
    

    merge_major(@majors);
    publish_all(@majors);
}

sub publish_all {
    my @versions = @_;
    foreach my $version (@versions) {
    	say "[+] Publish:$version";
        my $sh = <<SHELL
            git checkout spark-$version
            git tag v$release-$version
            sbt +test +publishSigned
SHELL
            ;
        _exec_shell($sh)
    }
}

sub merge_major {
    my @versions = @_;

    my @minors = keys %{{map{/(\d.\d)/;$1 => 1} @versions}};
    foreach my $minor (@minors) {
        my ($last_version) =  (sort {$b cmp $a} grep {/\Q$minor/} @versions);
        foreach my $version (grep {!/$last_version/} @versions) {
    	    say "[+] Merge:$version";
            my $sh = <<SHELL
                git checkout spark-$version
                git merge spark-$last_version
                git push
SHELL
;
            _exec_shell($sh);
        }
    }
}

sub _exec_shell {
    my $sh = shift;
    say qx{$sh};
    die $! if $!;
}
