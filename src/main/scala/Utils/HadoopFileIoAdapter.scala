package Utils

/**
  * Created by baronfeng on 2017/10/20.
  * IIOAdapter接口的实现，给hanlp用的
  *
  */

import java.net.URI

import com.hankcs.hanlp.corpus.io.IIOAdapter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HadoopFileIoAdapter extends IIOAdapter {
  @Override
  def open(path: String): java.io.InputStream = {
    val conf: Configuration = new Configuration()
    val fs: FileSystem = FileSystem.get(URI.create(path), conf)
    fs.open(new Path(path))
  }

  @Override
  def create(path: String): java.io.OutputStream = {
    val conf: Configuration = new Configuration()
    val fs: FileSystem = FileSystem.get(URI.create(path), conf)
    fs.create(new Path(path))
  }

}
