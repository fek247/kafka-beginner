// package Common;

// import java.io.BufferedReader;
// import java.io.DataInputStream;
// import java.io.File;
// import java.io.FileInputStream;
// import java.io.FileReader;
// import java.io.IOException;
// import java.io.InputStream;
// import java.util.Arrays;

// public class MessageFile {
//     public void init(String path)
//     {
//         try {
//             // InputStream inputStream = new FileInputStream(new File(path));
//             // DataInputStream dataInputStream = new DataInputStream(inputStream);

//             // byte[] bytes = dataInputStream.readAllBytes();
//             // System.out.println(Arrays.toString(bytes));
//             FileReader fileReader = new FileReader(path);
//             BufferedReader reader = new BufferedReader(fileReader);
//             String line;
//             while ((line = reader.readLine()) != null) {
//                 System.out.println(line);
//             }
//         } catch (IOException e) {
//             e.printStackTrace();
//         }
//     }
// }
