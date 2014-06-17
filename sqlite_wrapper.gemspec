# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.name          = "sqlite_wrapper"
  gem.version       = OpencBot::VERSION
  gem.authors       = ["Shyam Peri"]
  gem.description   = %q{This gem is a wrapper gem which makes sqlite persistence easy with out a need to create or alter schema }
  gem.summary       = %q{Helper gem for persisting ruby objects to sqlite}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($/)

  gem.add_dependency "sqlite3"
end
