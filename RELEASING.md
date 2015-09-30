# Releasing Hermann

Hermann is a multi-platform gem, which means that two actual `.gem` files need
to be built and uploaded to [rubygems.org](https://rubygems.org/gems/hermann).

Here's the current process that [I](https://github.com/rtyler) use:

 * `rvm use ruby@rubygems` (*switch to MRI*)
 * `bundle install && rake` (*ensure that MRI tests pass*)
 * `rvm use jruby@rubygems` (*switch to JRuby*)
 * `bundle install && rake` (*ensure that the JRuby tests pass*)
 * `rake release` (*tag the release and upload the `-java` platform gem*)
 * `rvm use ruby@rubygems` (*switch back to MRI*)
 * `gem build hermann.gemspec` (*build the 'ruby' platform gem*)
 * `gem push pkg/hermann-0.blah.gem` (*upload the ruby platform gem*)

This can certainly be cleaned up, but this is the process at it is right now
