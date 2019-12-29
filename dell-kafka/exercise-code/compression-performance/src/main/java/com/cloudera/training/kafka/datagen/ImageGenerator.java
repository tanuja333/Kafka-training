package com.cloudera.training.kafka.datagen;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

/**
 * This class simulates the generation of an image that represents a still photo
 * from a surveillance camera, encoded as a byte array. However, for maximum
 * efficiency, the image is just dynamically generated, not an actual photo.
 */
public class ImageGenerator {

    private final int width;
    private final int height;
    private final float compressionLevel;

    /**
     * Creates a new ImageGenerator that will generate JPEG images of the
     * specified size, using the specified compression factor.
     *
     * @param width the width of the generated images, in pixels
     * @param height the height of the generated images, in pixels
     * @param compressionLevel the amount of JPEG compression to apply to the
     * generated images (0.0f represents the maximum possible compression, while
     * 1.0f represents minimal compression).
     */
    public ImageGenerator(int width, int height, float compressionLevel) {
        this.width = width;
        this.height = height;
        this.compressionLevel = compressionLevel;

        // Disabling the image IO cache speeds up JPEG conversion significantly
        ImageIO.setUseCache(false);
    }

    private BufferedImage generateImage() {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        ColorSelector colorSelector = new ColorSelector();
        int rgb = colorSelector.getRandomRgbValue();

        for (int x = 0; x < width; x++) {
            for (int y = 0; y < height; y++) {
                // switch colors at rare (but random) intervals
                if (Math.random() > 0.99995) {
                    rgb = colorSelector.getRandomRgbValue();
                }

                image.setRGB(x, y, rgb);
            }
        }

        return image;
    }

    public byte[] getJpegAsByteArray() throws IOException {
        BufferedImage image = generateImage();

        ImageReader jpgReader = ImageIO.getImageReadersBySuffix("jpg").next();
        ImageWriter jpgWriter = ImageIO.getImageWriter(jpgReader);

        byte[] retval;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            ImageOutputStream ios = ImageIO.createImageOutputStream(baos);
            jpgWriter.setOutput(ios);

            ImageWriteParam param = jpgWriter.getDefaultWriteParam();
            param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            param.setCompressionQuality(compressionLevel); // no compression

            IIOImage outputImage = new IIOImage(image, null, null);
            jpgWriter.write(null, outputImage, param);
            baos.flush();
            jpgWriter.dispose();

            retval = baos.toByteArray();
        }

        return retval;
    }

    private static class ColorSelector {

        private static final int[] COLORS = new int[]{
            Color.RED.getRGB(),
            Color.BLACK.getRGB(),
            Color.BLUE.getRGB(),
            Color.GREEN.getRGB(),
            Color.YELLOW.getRGB()
        };

        private final Random random;

        ColorSelector() {
            this.random = new Random();
        }

        int getRandomRgbValue() {
            int index = random.nextInt(COLORS.length);
            return COLORS[index];
        }
    }
}
