const webpackConfigDev = require('../../webpack.config.dev')

module.exports = function(grunt) {

  grunt.config.set('webpack', {
    // prod: webpackConfigProd, // Uncomment entry here to use a different config for production
    dev: webpackConfigDev
  })

  grunt.loadNpmTasks('grunt-webpack')
};
