const path = require('path')

module.exports = {
  mode: 'development',
  entry: './assets/js/index.js',
  output: {
    filename: 'app.js',
    path: path.resolve(__dirname, '.tmp/public/js')
  },
  module: {
    rules: [
      {
        test: /\.css$/,
        use: ['style-loader', 'css-loader']
      }
    ]
  }
}
